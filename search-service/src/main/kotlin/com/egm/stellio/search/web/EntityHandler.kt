package com.egm.stellio.search.web

import arrow.core.getOrElse
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.model.hasSuccessfulUpdate
import com.egm.stellio.search.service.EntityEventService
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.removeContextFromInput
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
import java.util.Optional

@RestController
@RequestMapping("/ngsi-ld/v1/entities")
class EntityHandler(
    private val applicationProperties: ApplicationProperties,
    private val entityPayloadService: EntityPayloadService,
    private val queryService: QueryService,
    private val authorizationService: AuthorizationService,
    private val entityEventService: EntityEventService
) : BaseHandler() {

    /**
     * Implements 6.4.3.1 - Create Entity
     */
    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) = extractPayloadAndContexts(requestBody, httpHeaders).bind()
        val jsonLdEntity = expandJsonLdEntity(body, contexts)
        val ngsiLdEntity = jsonLdEntity.toNgsiLdEntity().bind()

        authorizationService.userCanCreateEntities(sub).bind()
        entityPayloadService.checkEntityExistence(ngsiLdEntity.id, true).bind()

        entityPayloadService.createEntity(
            ngsiLdEntity,
            jsonLdEntity,
            sub.getOrNull()
        ).bind()
        authorizationService.createAdminRight(ngsiLdEntity.id, sub).bind()

        entityEventService.publishEntityCreateEvent(
            sub.getOrNull(),
            ngsiLdEntity.id,
            ngsiLdEntity.types,
            contexts
        )

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
    @PatchMapping("/{entityId}", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun merge(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @RequestParam options: MultiValueMap<String, String>,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        entityPayloadService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub).bind()

        val (body, contexts) = extractPayloadAndContexts(requestBody, httpHeaders).bind()

        val observedAt = options.getFirst(QUERY_PARAM_OPTIONS_OBSERVEDAT_VALUE)
            ?.parseTimeParameter("'observedAt' parameter is not a valid date")
            ?.getOrElse { return@either BadRequestDataException(it).left().bind<ResponseEntity<*>>() }

        val expandedAttributes = expandAttributes(body, contexts)

        val updateResult = entityPayloadService.mergeEntity(
            entityId,
            expandedAttributes,
            observedAt,
            sub.getOrNull()
        ).bind()

        if (updateResult.updated.isNotEmpty()) {
            entityEventService.publishAttributeChangeEvents(
                sub.getOrNull(),
                entityId,
                expandedAttributes,
                updateResult,
                true,
                contexts
            )
        }

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
    @PutMapping("/{entityId}", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun replace(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) = extractPayloadAndContexts(requestBody, httpHeaders).bind()

        val jsonLdEntity = expandJsonLdEntity(body, contexts)
        val ngsiLdEntity = jsonLdEntity.toNgsiLdEntity().bind()

        entityPayloadService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub).bind()

        if (ngsiLdEntity.id != entityId)
            BadRequestDataException("The id contained in the body is not the same as the one provided in the URL")
                .left().bind<ResponseEntity<*>>()

        entityPayloadService.replaceEntity(
            entityId,
            ngsiLdEntity,
            jsonLdEntity,
            sub.getOrNull()
        ).bind()

        entityEventService.publishEntityReplaceEvent(
            sub.getOrNull(),
            ngsiLdEntity.id,
            ngsiLdEntity.types,
            contexts
        )

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
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val mediaType = getApplicableMediaType(httpHeaders)
        val sub = getSubFromSecurityContext()

        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders).bind()
        val queryParams = parseQueryParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        ).bind()

        if (
            queryParams.ids.isEmpty() &&
            queryParams.q.isNullOrEmpty() &&
            queryParams.type.isNullOrEmpty() &&
            queryParams.attrs.isEmpty()
        )
            BadRequestDataException(
                "one of 'ids', 'q', 'type' and 'attrs' request parameters have to be specified"
            ).left().bind<ResponseEntity<*>>()

        val accessRightFilter = authorizationService.computeAccessRightFilter(sub)
        val countAndEntities = queryService.queryEntities(queryParams, accessRightFilter).bind()

        val filteredEntities =
            countAndEntities.first.filter { it.containsAnyOf(queryParams.attrs) }
                .map {
                    JsonLdEntity(
                        JsonLdUtils.filterJsonLdEntityOnAttributes(it, queryParams.attrs),
                        it.contexts
                    )
                }

        val compactedEntities = JsonLdUtils.compactEntities(
            filteredEntities,
            queryParams.useSimplifiedRepresentation,
            contextLink,
            mediaType
        ).map {
            if (!queryParams.includeSysAttrs)
                it.withoutSysAttrs()
            else it
        }

        buildQueryResponse(
            compactedEntities,
            countAndEntities.second,
            "/ngsi-ld/v1/entities",
            queryParams,
            params,
            mediaType,
            contextLink
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.5.3.1 - Retrieve Entity
     */
    @GetMapping("/{entityId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByURI(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val mediaType = getApplicableMediaType(httpHeaders)
        val sub = getSubFromSecurityContext()

        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders).bind()
        val queryParams = parseQueryParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        ).bind()

        entityPayloadService.checkEntityExistence(entityId).bind()

        authorizationService.userCanReadEntity(entityId, sub).bind()

        val jsonLdEntity = queryService.queryEntity(entityId, listOf(contextLink)).bind()

        jsonLdEntity.checkContainsAnyOf(queryParams.attrs).bind()

        val filteredJsonLdEntity = JsonLdEntity(
            JsonLdUtils.filterJsonLdEntityOnAttributes(jsonLdEntity, queryParams.attrs),
            jsonLdEntity.contexts
        )
        val compactedEntity = JsonLdUtils.compact(filteredJsonLdEntity, contextLink, mediaType).toMutableMap()

        prepareGetSuccessResponse(mediaType, contextLink).body(
            serializeObject(
                compactedEntity.toFinalRepresentation(
                    queryParams.includeSysAttrs,
                    queryParams.useSimplifiedRepresentation
                )
            )
        )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.5.3.2 - Delete Entity
     */
    @DeleteMapping("/{entityId}")
    suspend fun delete(
        @PathVariable entityId: URI
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        entityPayloadService.checkEntityExistence(entityId).bind()
        // Is there a way to avoid loading the entity to get its type and contexts (for the event to be published)?
        val entity = entityPayloadService.retrieve(entityId).bind()
        authorizationService.userCanAdminEntity(entityId, sub).bind()

        entityPayloadService.deleteEntity(entityId).bind()
        authorizationService.removeRightsOnEntity(entityId).bind()

        entityEventService.publishEntityDeleteEvent(sub.getOrNull(), entityId, entity.types, entity.contexts)

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @DeleteMapping("/", "")
    fun handleMissingEntityIdOnDelete(): ResponseEntity<*> =
        missingPathErrorResponse("Missing entity id when trying to delete an entity")

    /**
     * Implements 6.6.3.1 - Append Entity Attributes
     *
     */
    @PostMapping("/{entityId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun appendEntityAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @RequestParam options: Optional<String>,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val disallowOverwrite = options.map { it == QUERY_PARAM_OPTIONS_NOOVERWRITE_VALUE }.orElse(false)

        entityPayloadService.checkEntityExistence(entityId).bind()

        val (body, contexts) = extractPayloadAndContexts(requestBody, httpHeaders).bind()
        val expandedAttributes = expandAttributes(body, contexts)

        authorizationService.userCanUpdateEntity(entityId, sub).bind()

        val updateResult = entityPayloadService.appendAttributes(
            entityId,
            expandedAttributes,
            disallowOverwrite,
            sub.getOrNull()
        ).bind()

        if (updateResult.hasSuccessfulUpdate()) {
            entityEventService.publishAttributeChangeEvents(
                sub.getOrNull(),
                entityId,
                expandedAttributes,
                updateResult,
                true,
                contexts
            )
        }

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
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    suspend fun updateEntityAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) = extractPayloadAndContexts(requestBody, httpHeaders).bind()
        val expandedAttributes = expandAttributes(body, contexts)

        entityPayloadService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub).bind()

        val updateResult = entityPayloadService.updateAttributes(
            entityId,
            expandedAttributes,
            sub.getOrNull()
        ).bind()

        if (updateResult.updated.isNotEmpty()) {
            entityEventService.publishAttributeChangeEvents(
                sub.getOrNull(),
                entityId,
                expandedAttributes,
                updateResult,
                true,
                contexts
            )
        }

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
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    suspend fun partialAttributeUpdate(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: URI,
        @PathVariable attrId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()

        entityPayloadService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub).bind()

        // We expect an NGSI-LD Attribute Fragment which should be a JSON-LD Object (see 5.4)
        val (body, contexts) = extractPayloadAndContexts(requestBody, httpHeaders).bind()

        val expandedAttribute = expandAttribute(attrId, removeContextFromInput(body), contexts)

        entityPayloadService.partialUpdateAttribute(
            entityId,
            expandedAttribute,
            sub.getOrNull()
        )
            .bind()
            .let {
                if (it.updated.isEmpty())
                    ResourceNotFoundException("Unknown attribute in entity $entityId").left()
                else {
                    entityEventService.publishAttributeChangeEvents(
                        sub.getOrNull(),
                        entityId,
                        expandedAttribute.toExpandedAttributes(),
                        it,
                        false,
                        contexts
                    )

                    ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>().right()
                }
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
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val deleteAll = params.getFirst("deleteAll")?.toBoolean() ?: false
        val datasetId = params.getFirst("datasetId")?.toUri()

        val contexts = listOf(getContextFromLinkHeaderOrDefault(httpHeaders).bind())
        val expandedAttrId = JsonLdUtils.expandJsonLdTerm(attrId, contexts)

        authorizationService.userCanUpdateEntity(entityId, sub).bind()

        entityPayloadService.deleteAttribute(
            entityId,
            expandedAttrId,
            datasetId,
            deleteAll
        ).bind()

        entityEventService.publishAttributeDeleteEvent(
            sub.getOrNull(),
            entityId,
            expandedAttrId,
            datasetId,
            deleteAll,
            contexts
        )
        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    @DeleteMapping("/attrs/{attrId}", "/{entityId}/attrs")
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
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> = either {
        val sub = getSubFromSecurityContext()
        val (body, contexts) = extractPayloadAndContexts(requestBody, httpHeaders).bind()

        entityPayloadService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub).bind()

        val expandedAttribute = expandAttribute(attrId, removeContextFromInput(body), contexts)

        entityPayloadService.replaceAttribute(entityId, expandedAttribute, sub.getOrNull()).bind()
            .let {
                if (it.updated.isEmpty())
                    ResourceNotFoundException(it.notUpdated[0].reason).left()
                else {
                    entityEventService.publishAttributeChangeEvents(
                        sub.getOrNull(),
                        entityId,
                        expandedAttribute.toExpandedAttributes(),
                        it,
                        false,
                        contexts
                    )

                    ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>().right()
                }
            }.bind()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
