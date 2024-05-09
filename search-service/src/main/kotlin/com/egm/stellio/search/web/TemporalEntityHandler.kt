package com.egm.stellio.search.web

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.service.*
import com.egm.stellio.search.util.composeTemporalEntitiesQuery
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.web.BaseHandler
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.net.URI

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entities")
class TemporalEntityHandler(
    private val entityPayloadService: EntityPayloadService,
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val queryService: QueryService,
    private val authorizationService: AuthorizationService,
    private val applicationProperties: ApplicationProperties
) : BaseHandler() {

    /**
     * Implements 6.18.3.1 - Create or Update (Upsert) Temporal Representation of Entity
     */
    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()

        val jsonLdTemporalEntity = expandJsonLdEntity(body, contexts)
        val entityUri = jsonLdTemporalEntity.id.toUri()
        val entityDoesNotExist = entityPayloadService.checkEntityExistence(entityUri, true).isRight()

        val jsonLdInstances = jsonLdTemporalEntity.getAttributes()
        jsonLdInstances.checkTemporalAttributeInstance().bind()
        val sortedJsonLdInstances = jsonLdInstances.sorted()

        if (entityDoesNotExist) {
            authorizationService.userCanCreateEntities(sub).bind()

            // create a view of the entity containing only the most recent instance of each attribute
            val expandedEntity = ExpandedEntity(
                sortedJsonLdInstances
                    .keepFirstInstances()
                    .addCoreMembers(jsonLdTemporalEntity.id, jsonLdTemporalEntity.types)
            )
            val ngsiLdEntity = expandedEntity.toNgsiLdEntity().bind()

            entityPayloadService.createEntity(ngsiLdEntity, expandedEntity, sub.getOrNull()).bind()
            entityPayloadService.upsertAttributes(
                entityUri,
                sortedJsonLdInstances.removeFirstInstances(),
                sub.getOrNull()
            ).bind()
            authorizationService.createAdminRight(entityUri, sub).bind()

            ResponseEntity.status(HttpStatus.CREATED)
                .location(URI("/ngsi-ld/v1/temporal/entities/$entityUri"))
                .build<String>()
        } else {
            authorizationService.userCanUpdateEntity(entityUri, sub).bind()
            entityPayloadService.upsertAttributes(
                entityUri,
                sortedJsonLdInstances,
                sub.getOrNull()
            ).bind()

            ResponseEntity.status(HttpStatus.NO_CONTENT).build()
        }
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
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        entityPayloadService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub).bind()

        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()
        val jsonLdInstances = expandAttributes(body, contexts)
        jsonLdInstances.checkTemporalAttributeInstance().bind()
        val sortedJsonLdInstances = jsonLdInstances.sorted()

        entityPayloadService.upsertAttributes(
            entityId,
            sortedJsonLdInstances,
            sub.getOrNull()
        ).bind()

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
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()

        val temporalEntitiesQuery =
            composeTemporalEntitiesQuery(applicationProperties.pagination, params, contexts, true).bind()

        val accessRightFilter = authorizationService.computeAccessRightFilter(sub)

        val (temporalEntities, total) = queryService.queryTemporalEntities(
            temporalEntitiesQuery,
            accessRightFilter
        ).bind()

        val compactedEntities = compactEntities(temporalEntities, contexts)

        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType)
        buildQueryResponse(
            compactedEntities.toFinalRepresentation(ngsiLdDataRepresentation),
            total,
            "/ngsi-ld/v1/temporal/entities",
            temporalEntitiesQuery.entitiesQuery.paginationQuery,
            params,
            mediaType,
            contexts
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
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        entityPayloadService.checkEntityExistence(entityId).bind()

        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()

        authorizationService.userCanReadEntity(entityId, sub).bind()

        val temporalEntitiesQuery =
            composeTemporalEntitiesQuery(applicationProperties.pagination, params, contexts).bind()

        val temporalEntity = queryService.queryTemporalEntity(entityId, temporalEntitiesQuery).bind()

        val compactedEntity = compactEntity(temporalEntity, contexts)

        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType)
        prepareGetSuccessResponseHeaders(mediaType, contexts)
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
        @PathVariable instanceId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()
        val instanceUri = instanceId.toUri()
        attrId.checkNameIsNgsiLdSupported().bind()

        entityPayloadService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub).bind()

        val expandedAttribute = expandAttribute(attrId, body, contexts)
        expandedAttribute.toExpandedAttributes().checkTemporalAttributeInstance().bind()

        attributeInstanceService.modifyAttributeInstance(
            entityId,
            expandedAttribute.first,
            instanceUri,
            expandedAttribute.second
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
        @PathVariable entityId: URI
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        entityPayloadService.checkEntityExistence(entityId).bind()
        authorizationService.userCanAdminEntity(entityId, sub).bind()
        entityPayloadService.deleteEntity(entityId).bind()
        authorizationService.removeRightsOnEntity(entityId).bind()

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
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val deleteAll = params.getFirst("deleteAll")?.toBoolean() ?: false
        val datasetId = params.getFirst("datasetId")?.toUri()

        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        attrId.checkNameIsNgsiLdSupported().bind()
        val expandedAttrId = expandJsonLdTerm(attrId, contexts)

        temporalEntityAttributeService.checkEntityAndAttributeExistence(
            entityId,
            expandedAttrId,
            datasetId
        ).bind()

        authorizationService.userCanUpdateEntity(entityId, sub).bind()

        entityPayloadService.deleteAttribute(
            entityId,
            expandedAttrId,
            datasetId,
            deleteAll
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
        @PathVariable instanceId: URI
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        attrId.checkNameIsNgsiLdSupported().bind()
        val expandedAttrId = expandJsonLdTerm(attrId, contexts)

        entityPayloadService.checkEntityExistence(entityId).bind()

        authorizationService.userCanUpdateEntity(entityId, sub).bind()

        attributeInstanceService.deleteInstance(entityId, expandedAttrId, instanceId).bind()

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

    private fun ExpandedAttributes.sorted(): ExpandedAttributes =
        this.mapValues {
            it.value.sortedByDescending { expandedAttributePayloadEntry ->
                expandedAttributePayloadEntry.getMemberValueAsDateTime(NGSILD_OBSERVED_AT_PROPERTY)
            }
        }

    private fun ExpandedAttributes.keepFirstInstances(): ExpandedAttributes =
        this.mapValues { listOf(it.value.first()) }

    private fun ExpandedAttributes.removeFirstInstances(): ExpandedAttributes =
        this.mapValues {
            it.value.drop(1)
        }
}
