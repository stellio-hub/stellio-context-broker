package com.egm.stellio.search.entity.web

import arrow.core.getOrElse
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import arrow.core.separateEither
import arrow.fx.coroutines.parMap
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.csr.model.addWarnings
import com.egm.stellio.search.csr.service.ContextSourceCaller
import com.egm.stellio.search.csr.service.ContextSourceRegistrationService
import com.egm.stellio.search.csr.service.ContextSourceUtils
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.search.entity.service.LinkedEntityService
import com.egm.stellio.search.entity.util.composeEntitiesQueryFromGet
import com.egm.stellio.search.entity.util.validateMinimalQueryEntitiesParameters
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.NgsiLdDataRepresentation.Companion.parseRepresentations
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.filterAttributes
import com.egm.stellio.shared.model.parameter.AllowedParameters
import com.egm.stellio.shared.model.parameter.QP
import com.egm.stellio.shared.model.parameter.QueryParameter
import com.egm.stellio.shared.model.toFinalRepresentation
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.GEO_JSON_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_MERGE_PATCH_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.buildQueryResponse
import com.egm.stellio.shared.util.extractPayloadAndContexts
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.missingPathErrorResponse
import com.egm.stellio.shared.util.parseTimeParameter
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.BaseHandler
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.net.URI

@RestController
@RequestMapping("/ngsi-ld/v1/entities")
@Validated
class EntityHandler(
    private val applicationProperties: ApplicationProperties,
    private val entityService: EntityService,
    private val entityQueryService: EntityQueryService,
    private val contextSourceRegistrationService: ContextSourceRegistrationService,
    private val linkedEntityService: LinkedEntityService
) : BaseHandler() {

    /**
     * Implements 6.4.3.1 - Create Entity
     */
    @PostMapping(consumes = [APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters(implemented = [], notImplemented = [QueryParameter.LOCAL, QueryParameter.VIA])
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()
        val expandedEntity = expandJsonLdEntity(body, contexts)
        val ngsiLdEntity = expandedEntity.toNgsiLdEntity().bind()

        entityService.createEntity(ngsiLdEntity, expandedEntity, sub.getOrNull()).bind()

        ResponseEntity.status(HttpStatus.CREATED)
            .location(URI("/ngsi-ld/v1/entities/${ngsiLdEntity.id}"))
            .build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.5.3.4 - Merge Entity
     */
    @PatchMapping("/{entityId}", consumes = [APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun merge(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @RequestParam
        @AllowedParameters(
            implemented = [QP.OPTIONS, QP.TYPE, QP.OBSERVED_AT, QP.LANG],
            notImplemented = [QP.FORMAT, QP.LOCAL, QP.VIA]
        )
        options: MultiValueMap<String, String>,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()
        val observedAt = options.getFirst(QueryParameter.OBSERVED_AT.key)
            ?.parseTimeParameter("'observedAt' parameter is not a valid date")
            ?.getOrElse { return@either BadRequestDataException(it).left().bind<ResponseEntity<*>>() }
        val expandedAttributes = expandAttributes(body, contexts)

        val updateResult = entityService.mergeEntity(
            entityId,
            expandedAttributes,
            observedAt,
            sub.getOrNull()
        ).bind()

        if (updateResult.notUpdated.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(updateResult)
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PatchMapping("/", "")
    fun handleMissingEntityIdOnMerge(): ResponseEntity<*> =
        missingPathErrorResponse("Missing entity id when trying to merge an entity")

    /**
     * Implements 6.5.3.3 - Replace Entity
     */
    @PutMapping("/{entityId}", consumes = [APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun replace(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters(implemented = [], notImplemented = [QueryParameter.LOCAL, QueryParameter.VIA])
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()
        val expandedEntity = expandJsonLdEntity(body, contexts)
        val ngsiLdEntity = expandedEntity.toNgsiLdEntity().bind()

        if (ngsiLdEntity.id != entityId)
            BadRequestDataException("The id contained in the body is not the same as the one provided in the URL")
                .left().bind<ResponseEntity<*>>()

        entityService.replaceEntity(
            entityId,
            ngsiLdEntity,
            expandedEntity,
            sub.getOrNull()
        ).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PutMapping("/", "")
    fun handleMissingEntityIdOnReplace(): ResponseEntity<*> =
        missingPathErrorResponse("Missing entity id when trying to replace an entity")

    /**
     * Implements 6.4.3.2 - Query Entities
     */
    @GetMapping(produces = [APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, GEO_JSON_CONTENT_TYPE])
    suspend fun getEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam
        @AllowedParameters(
            implemented = [
                QP.OPTIONS, QP.FORMAT, QP.COUNT, QP.OFFSET, QP.LIMIT, QP.ID, QP.TYPE, QP.ID_PATTERN, QP.ATTRS, QP.Q,
                QP.GEOMETRY, QP.GEOREL, QP.COORDINATES, QP.GEOPROPERTY, QP.GEOMETRY_PROPERTY,
                QP.LANG, QP.SCOPEQ, QP.CONTAINED_BY, QP.JOIN, QP.JOIN_LEVEL, QP.DATASET_ID,
            ],
            notImplemented = [QP.PICK, QP.OMIT, QP.EXPAND_VALUES, QP.CSF, QP.ENTITY_MAP, QP.LOCAL, QP.VIA]
        )
        params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val sub = getSubFromSecurityContext()

        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val entitiesQuery = composeEntitiesQueryFromGet(
            applicationProperties.pagination,
            params,
            contexts
        ).bind()
            .validateMinimalQueryEntitiesParameters().bind()

        val (entities, count) = entityQueryService.queryEntities(entitiesQuery, sub.getOrNull()).bind()

        val filteredEntities = entities.filterAttributes(entitiesQuery.attrs, entitiesQuery.datasetId)

        val compactedEntities =
            compactEntities(filteredEntities, contexts).let {
                linkedEntityService.processLinkedEntities(it, entitiesQuery, sub.getOrNull()).bind()
            }

        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType)
        buildQueryResponse(
            compactedEntities.toFinalRepresentation(ngsiLdDataRepresentation),
            count,
            "/ngsi-ld/v1/entities",
            entitiesQuery.paginationQuery,
            params,
            mediaType,
            contexts
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.5.3.1 - Retrieve Entity
     */
    @GetMapping("/{entityId}", produces = [APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, GEO_JSON_CONTENT_TYPE])
    suspend fun getByURI(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @RequestParam
        @AllowedParameters(
            implemented = [
                QP.OPTIONS, QP.FORMAT, QP.TYPE, QP.ATTRS, QP.GEOMETRY_PROPERTY,
                QP.LANG, QP.CONTAINED_BY, QP.JOIN, QP.JOIN_LEVEL, QP.DATASET_ID,
            ],
            notImplemented = [QP.PICK, QP.OMIT, QP.ENTITY_MAP, QP.LOCAL, QueryParameter.VIA]
        )
        params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val sub = getSubFromSecurityContext()

        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val entitiesQuery = composeEntitiesQueryFromGet(
            applicationProperties.pagination,
            params,
            contexts
        ).bind()

        val csrFilters =
            CSRFilters(ids = setOf(entityId), operations = listOf(Operation.FEDERATION_OPS, Operation.RETRIEVE_ENTITY))

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(csrFilters)

        val localEntity = either {
            val expandedEntity = entityQueryService.queryEntity(entityId, sub.getOrNull()).bind()
            expandedEntity.checkContainsAnyOf(entitiesQuery.attrs).bind()

            val filteredExpandedEntity = ExpandedEntity(
                expandedEntity.filterAttributes(entitiesQuery.attrs, entitiesQuery.datasetId)
            )
            compactEntity(filteredExpandedEntity, contexts)
        }

        // we can add parMap(concurrency = X) if this trigger too much http connexion at the same time
        val (warnings, remoteEntitiesWithCSR) = matchingCSR.parMap { csr ->
            val response = ContextSourceCaller.getDistributedInformation(
                httpHeaders,
                csr,
                "/ngsi-ld/v1/entities/$entityId",
                params
            )
            contextSourceRegistrationService.updateContextSourceStatus(csr, response.isRight())
            response.map { it?.let { it to csr } }
        }.separateEither()
            .let { (warnings, maybeResponses) ->
                warnings.toMutableList() to maybeResponses.filterNotNull()
            }

        // we could simplify the code if we check the JsonPayload beforehand
        val (mergeWarnings, mergedEntity) = ContextSourceUtils.mergeEntities(
            localEntity.getOrNull(),
            remoteEntitiesWithCSR
        ).toPair()

        mergeWarnings?.let { warnings.addAll(it) }

        if (mergedEntity == null) {
            val localError = localEntity.leftOrNull()
            return localError!!.toErrorResponse().addWarnings(warnings)
        }

        val mergedEntityWithLinkedEntities =
            linkedEntityService.processLinkedEntities(mergedEntity, entitiesQuery, sub.getOrNull()).bind()

        val ngsiLdDataRepresentation = parseRepresentations(params, mediaType)
        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .let {
                val body = if (mergedEntityWithLinkedEntities.size == 1)
                    serializeObject(mergedEntityWithLinkedEntities[0].toFinalRepresentation(ngsiLdDataRepresentation))
                else
                    serializeObject(mergedEntityWithLinkedEntities.toFinalRepresentation(ngsiLdDataRepresentation))
                it.body(body)
            }
            .addWarnings(warnings)
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.5.3.2 - Delete Entity
     */
    @DeleteMapping("/{entityId}")
    suspend fun delete(
        @PathVariable entityId: URI,
        @AllowedParameters(implemented = [], notImplemented = [QP.LOCAL, QP.VIA])
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
     * Implements 6.6.3.1 - Append Entity Attributes
     *
     */
    @PostMapping("/{entityId}/attrs", consumes = [APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun appendEntityAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @RequestBody requestBody: Mono<String>,
        @RequestParam
        @AllowedParameters(
            implemented = [QP.OPTIONS],
            notImplemented = [QP.TYPE, QP.LOCAL, QP.VIA] // type is for dist-ops
        )
        params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val options = params.getFirst(QueryParameter.OPTIONS.key)
        val sub = getSubFromSecurityContext()
        val disallowOverwrite = options?.let { it == QueryParameter.NO_OVERWRITE.key } ?: false

        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()
        val expandedAttributes = expandAttributes(body, contexts)

        val updateResult = entityService.appendAttributes(
            entityId,
            expandedAttributes,
            disallowOverwrite,
            sub.getOrNull()
        ).bind()

        if (updateResult.notUpdated.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(updateResult)
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PostMapping("/attrs")
    fun handleMissingEntityIdOnAttributeAppend(): ResponseEntity<*> =
        missingPathErrorResponse("Missing entity id when trying to append attribute")

    /**
     * Implements 6.6.3.2 - Update Entity Attributes
     *
     */
    @PatchMapping(
        "/{entityId}/attrs",
        consumes = [APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    suspend fun updateEntityAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters(implemented = [], notImplemented = [QP.LOCAL, QP.VIA])
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()
        val expandedAttributes = expandAttributes(body, contexts)

        val updateResult = entityService.updateAttributes(
            entityId,
            expandedAttributes,
            sub.getOrNull()
        ).bind()

        if (updateResult.notUpdated.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(updateResult)
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PatchMapping("/attrs")
    fun handleMissingAttributeOnAttributeUpdate(): ResponseEntity<*> =
        missingPathErrorResponse("Missing attribute id when trying to update an attribute")

    /**
     * Implements 6.7.3.1 - Partial Attribute Update
     *
     */
    @PatchMapping(
        "/{entityId}/attrs/{attrId}",
        consumes = [APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    suspend fun partialAttributeUpdate(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @PathVariable attrId: String,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters(implemented = [], notImplemented = [QP.LOCAL, QP.VIA])
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        // We expect an NGSI-LD Attribute Fragment which should be a JSON-LD Object (see 5.4)
        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()

        val expandedAttribute = expandAttribute(attrId, body, contexts)

        entityService.partialUpdateAttribute(
            entityId,
            expandedAttribute,
            sub.getOrNull()
        )
            .bind()
            .let {
                if (it.updated.isEmpty())
                    ResourceNotFoundException("Unknown attribute in entity $entityId").left()
                else
                    ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>().right()
            }.bind()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @PatchMapping("/attrs/{attrId}")
    fun handleMissingAttributeOnPartialAttributeUpdate(): ResponseEntity<*> =
        missingPathErrorResponse("Missing attribute id when trying to partially update an attribute")

    /**
     * Implements 6.7.3.2 - Delete Entity Attribute
     */
    @DeleteMapping("/{entityId}/attrs/{attrId}")
    suspend fun deleteEntityAttribute(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @PathVariable attrId: String,
        @AllowedParameters(
            implemented = [QP.DELETE_ALL, QP.TYPE, QP.DATASET_ID],
            notImplemented = [QP.LOCAL, QP.VIA]
        )
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val deleteAll = params.getFirst(QueryParameter.DELETE_ALL.key)?.toBoolean() ?: false
        val datasetId = params.getFirst(QueryParameter.DATASET_ID.key)?.toUri()

        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
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

    @DeleteMapping("/attrs/{attrId}")
    fun handleMissingEntityIdOrAttributeOnDeleteAttribute(): ResponseEntity<*> =
        missingPathErrorResponse("Missing entity id or attribute id when trying to delete an attribute")

    /**
     * Implements 6.7.3.3 - Replace Attribute
     */
    @PutMapping("/{entityId}/attrs/{attrId}")
    suspend fun replaceEntityAttribute(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @PathVariable attrId: String,
        @RequestBody requestBody: Mono<String>,
        @AllowedParameters(implemented = [], notImplemented = [QP.LOCAL, QP.VIA])
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) =
            extractPayloadAndContexts(requestBody, httpHeaders, applicationProperties.contexts.core).bind()

        val expandedAttribute = expandAttribute(attrId, body, contexts)

        entityService.replaceAttribute(entityId, expandedAttribute, sub.getOrNull()).bind()
            .let {
                if (it.updated.isEmpty())
                    ResourceNotFoundException(it.notUpdated[0].reason).left()
                else
                    ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>().right()
            }.bind()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
