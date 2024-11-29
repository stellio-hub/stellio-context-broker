package com.egm.stellio.search.temporal.web

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.search.temporal.service.TemporalQueryService
import com.egm.stellio.search.temporal.service.TemporalService
import com.egm.stellio.search.temporal.util.composeTemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.web.TemporalApiResponses.buildEntitiesTemporalResponse
import com.egm.stellio.search.temporal.web.TemporalApiResponses.buildEntityTemporalResponse
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedAttributes
import com.egm.stellio.shared.model.NgsiLdDataRepresentation.Companion.parseRepresentations
import com.egm.stellio.shared.model.getMemberValueAsDateTime
import com.egm.stellio.shared.model.parameter.AllowedParameters
import com.egm.stellio.shared.model.parameter.QP
import com.egm.stellio.shared.model.toExpandedAttributes
import com.egm.stellio.shared.model.toFinalRepresentation
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_MERGE_PATCH_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.checkNameIsNgsiLdSupported
import com.egm.stellio.shared.util.extractPayloadAndContexts
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.invalidTemporalInstanceMessage
import com.egm.stellio.shared.util.missingPathErrorResponse
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.BaseHandler
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.net.URI

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entities")
class TemporalEntityHandler(
    private val temporalService: TemporalService,
    private val temporalQueryService: TemporalQueryService,
    private val entityService: EntityService,
    private val applicationProperties: ApplicationProperties
) : BaseHandler() {

    /**
     * Implements 6.18.3.1 - Create or Update (Upsert) Temporal Representation of Entity
     */
    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters(notImplemented = [QP.LOCAL, QP.VIA])
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()

        val jsonLdTemporalEntity = expandJsonLdEntity(body, contexts)
        val entityUri = jsonLdTemporalEntity.id.toUri()

        val jsonLdInstances = jsonLdTemporalEntity.getAttributes()
        jsonLdInstances.checkTemporalAttributeInstance().bind()

        val result = temporalService.createOrUpdateTemporalEntity(
            entityUri,
            jsonLdTemporalEntity,
            sub.getOrNull()
        ).bind()

        if (result == TemporalService.CreateOrUpdateResult.CREATED)
            ResponseEntity.status(HttpStatus.CREATED)
                .location(URI("/ngsi-ld/v1/temporal/entities/$entityUri"))
                .build<String>()
        else
            ResponseEntity.status(HttpStatus.NO_CONTENT).build()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.20.3.1 - Add attributes to Temporal Representation of Entity
     */
    @PostMapping("/{entityId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun addAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters(notImplemented = [QP.LOCAL, QP.VIA])
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()
        val jsonLdInstances = expandAttributes(body, contexts)
        jsonLdInstances.checkTemporalAttributeInstance().bind()

        temporalService.upsertAttributes(entityId, jsonLdInstances, sub.getOrNull()).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PostMapping("/attrs")
    fun handleMissingEntityIdOnAttributeAppend(): ResponseEntity<*> =
        missingPathErrorResponse("Missing entity id when trying to append attribute")

    /**
     * Partial implementation of 6.18.3.2 - Query Temporal Evolution of Entities
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getForEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @AllowedParameters(
            implemented = [
                QP.OPTIONS, QP.FORMAT, QP.COUNT, QP.OFFSET, QP.LIMIT, QP.ID, QP.TYPE, QP.ID_PATTERN, QP.ATTRS, QP.Q,
                QP.GEOMETRY, QP.GEOREL, QP.COORDINATES, QP.GEOPROPERTY, QP.TIMEPROPERTY, QP.TIMEREL, QP.TIMEAT,
                QP.ENDTIMEAT, QP.LASTN, QP.LANG, QP.AGGRMETHODS, QP.AGGRPERIODDURATION, QP.SCOPEQ, QP.DATASET_ID
            ],
            notImplemented = [QP.LOCAL, QP.VIA, QP.PICK, QP.OMIT, QP.EXPAND_VALUES, QP.CSF]
        )
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()

        val temporalEntitiesQuery =
            composeTemporalEntitiesQueryFromGet(applicationProperties.pagination, params, contexts, true).bind()

        val (temporalEntities, total, range) = temporalQueryService.queryTemporalEntities(
            temporalEntitiesQuery,
            sub.getOrNull()
        ).bind()

        val compactedEntities = compactEntities(temporalEntities, contexts)

        buildEntitiesTemporalResponse(
            compactedEntities,
            total,
            "/ngsi-ld/v1/temporal/entities",
            temporalEntitiesQuery,
            params,
            mediaType,
            contexts,
            range
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Partial implementation of 6.19.3.1 (query parameters are not all supported)
     */
    @GetMapping("/{entityId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getForEntity(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @AllowedParameters(
            implemented = [
                QP.OPTIONS, QP.FORMAT, QP.ATTRS, QP.TIMEPROPERTY, QP.TIMEREL, QP.TIMEAT, QP.ENDTIMEAT, QP.LASTN,
                QP.LANG, QP.AGGRMETHODS, QP.AGGRPERIODDURATION, QP.DATASET_ID
            ],
            notImplemented = [QP.LOCAL, QP.VIA, QP.PICK, QP.OMIT]
        )
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()

        val temporalEntitiesQuery =
            composeTemporalEntitiesQueryFromGet(applicationProperties.pagination, params, contexts).bind()

        val (temporalEntity, range) = temporalQueryService.queryTemporalEntity(
            entityId,
            temporalEntitiesQuery,
            sub.getOrNull()
        ).bind()

        val compactedEntity = compactEntity(temporalEntity, contexts)

        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType)
        buildEntityTemporalResponse(mediaType, contexts, temporalEntitiesQuery, range)
            .body(serializeObject(compactedEntity.toFinalRepresentation(ngsiLdDataRepresentation)))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.22.3.1 - Modify Attribute Instance in Temporal Representation of Entity
     *
     */
    @PatchMapping(
        "/{entityId}/attrs/{attrId}/{instanceId}",
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    suspend fun modifyAttributeInstanceTemporal(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @PathVariable attrId: String,
        @PathVariable instanceId: URI,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters(notImplemented = [QP.LOCAL, QP.VIA])
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()
        attrId.checkNameIsNgsiLdSupported().bind()

        val expandedAttribute = expandAttribute(attrId, body, contexts)
        expandedAttribute.toExpandedAttributes().checkTemporalAttributeInstance().bind()

        temporalService.modifyAttributeInstance(
            entityId,
            instanceId,
            expandedAttribute,
            sub.getOrNull()
        ).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PatchMapping(
        "/attrs/{attrId}/{instanceId}",
        "/attrs/{instanceId}",
    )
    fun handleMissingParametersOnModifyInstanceTemporal(): ResponseEntity<*> =
        missingPathErrorResponse(
            "Missing some parameter (entity id, attribute id, instance id) when trying to modify temporal entity"
        )

    /**
     * Implements 6.19.3.2  - Delete Temporal Representation of an Entity
     */
    @DeleteMapping("/{entityId}")
    suspend fun deleteTemporalEntity(
        @PathVariable entityId: URI,
        @AllowedParameters(notImplemented = [QP.LOCAL, QP.VIA])
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        entityService.deleteEntity(entityId, sub.getOrNull()).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.21.3.1 - Delete Attribute from Temporal Representation of an Entity
     */
    @DeleteMapping("/{entityId}/attrs/{attrId}")
    suspend fun deleteAttributeTemporal(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @PathVariable attrId: String,
        @AllowedParameters(
            implemented = [QP.DELETE_ALL, QP.DATASET_ID],
            notImplemented = [QP.LOCAL, QP.VIA]
        )
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val deleteAll = params.getFirst(QP.DELETE_ALL.key)?.toBoolean() ?: false
        val datasetId = params.getFirst(QP.DATASET_ID.key)?.toUri()

        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        attrId.checkNameIsNgsiLdSupported().bind()
        val expandedAttrId = expandJsonLdTerm(attrId, contexts)

        entityService.deleteAttribute(
            entityId,
            expandedAttrId,
            datasetId,
            deleteAll,
            sub.getOrNull()
        ).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @DeleteMapping("/attrs/{attrId}", "/{entityId}/attrs")
    fun handleMissingEntityIdOrAttributeOnDeleteAttribute(): ResponseEntity<*> =
        missingPathErrorResponse("Missing entity id or attribute id when trying to delete an attribute temporal")

    /**
     * Implements 6.22.3.2 - Delete Attribute instance from Temporal Representation of an Entity
     */
    @DeleteMapping("/{entityId}/attrs/{attrId}/{instanceId}")
    suspend fun deleteAttributeInstanceTemporal(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @PathVariable attrId: String,
        @PathVariable instanceId: URI,
        @AllowedParameters(notImplemented = [QP.LOCAL, QP.VIA])
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        attrId.checkNameIsNgsiLdSupported().bind()
        val expandedAttrId = expandJsonLdTerm(attrId, contexts)

        temporalService.deleteAttributeInstance(entityId, expandedAttrId, instanceId, sub.getOrNull()).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @DeleteMapping("/attrs/{attrId}/{instanceId}")
    fun handleMissingEntityIdOrAttrOnDeleteAttrInstance(): ResponseEntity<*> =
        missingPathErrorResponse(
            "Missing entity, attribute or instance id when trying to delete an attribute instance"
        )

    private fun ExpandedAttributes.checkTemporalAttributeInstance(): Either<APIException, Unit> =
        this.values.all { expandedInstances ->
            expandedInstances.all { expandedAttributePayloadEntry ->
                expandedAttributePayloadEntry.getMemberValueAsDateTime(NGSILD_OBSERVED_AT_PROPERTY) != null
            }
        }.let {
            if (it) Unit.right()
            else BadRequestDataException(invalidTemporalInstanceMessage()).left()
        }
}
