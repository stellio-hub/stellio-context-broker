package com.egm.stellio.entity.web

import arrow.core.continuations.either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.config.ApplicationProperties
import com.egm.stellio.entity.service.EntityAttributeService
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.compact
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonLdUtils.filterJsonLdEntityOnAttributes
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import kotlinx.coroutines.reactive.awaitFirst
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
    private val entityService: EntityService,
    private val entityAttributeService: EntityAttributeService,
    private val authorizationService: AuthorizationService,
    private val entityEventService: EntityEventService
) {

    /**
     * Implements 6.4.3.1 - Create Entity
     */
    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body)
        val ngsiLdEntity = expandJsonLdEntity(body, contexts).toNgsiLdEntity()

        return either<APIException, ResponseEntity<*>> {
            authorizationService.isCreationAuthorized(ngsiLdEntity, sub).bind()
            val newEntityUri = entityService.createEntity(ngsiLdEntity)
            authorizationService.createAdminLink(newEntityUri, sub)

            entityEventService.publishEntityCreateEvent(
                sub.orNull(),
                ngsiLdEntity.id,
                ngsiLdEntity.type,
                contexts
            )

            ResponseEntity.status(HttpStatus.CREATED)
                .location(URI("/ngsi-ld/v1/entities/$newEntityUri"))
                .build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    /**
     * Implements 6.4.3.2 - Query Entities
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val count = params.getFirst(QUERY_PARAM_COUNT)?.toBoolean() ?: false
        val (offset, limit) = extractAndValidatePaginationParameters(
            params,
            applicationProperties.pagination.limitDefault,
            applicationProperties.pagination.limitMax,
            count
        )
        val ids = params.getFirst(QUERY_PARAM_ID)?.split(",")?.toListOfUri()
        val type = params.getFirst(QUERY_PARAM_TYPE)
        val idPattern = params.getFirst(QUERY_PARAM_ID_PATTERN)
        val attrs = params.getFirst(QUERY_PARAM_ATTRS)
        val q = params.getFirst(QUERY_PARAM_FILTER)
        val includeSysAttrs = params.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
            .contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
        val useSimplifiedRepresentation = params.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
            .contains(QUERY_PARAM_OPTIONS_KEYVALUES_VALUE)
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val sub = getSubFromSecurityContext()

        if (q == null && type == null && attrs == null)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(
                    BadRequestDataResponse(
                        "one of 'q', 'type' and 'attrs' request parameters have to be specified"
                    )
                )

        val expandedAttrs = parseAndExpandRequestParameter(attrs, contextLink)

        /**
         * Decoding query parameters is not supported by default so a call to a decode function was added query
         * with the right parameters values
         */
        val countAndEntities = entityService.searchEntities(
            QueryParams(ids, type?.let { expandJsonLdTerm(type, contextLink) }, idPattern, q?.decode(), expandedAttrs),
            sub,
            offset,
            limit,
            contextLink,
            includeSysAttrs
        )

        if (countAndEntities.second.isEmpty())
            return PagingUtils.buildPaginationResponse(
                serializeObject(emptyList<JsonLdEntity>()),
                countAndEntities.first,
                count,
                Pair(null, null),
                mediaType, contextLink
            )

        val filteredEntities =
            countAndEntities.second.filter { it.containsAnyOf(expandedAttrs) }
                .map {
                    JsonLdEntity(
                        filterJsonLdEntityOnAttributes(it, expandedAttrs),
                        it.contexts
                    )
                }

        val compactedEntities = compactEntities(filteredEntities, useSimplifiedRepresentation, contextLink, mediaType)
            .map { it.toMutableMap() }

        val prevAndNextLinks = PagingUtils.getPagingLinks(
            "/ngsi-ld/v1/entities",
            params,
            countAndEntities.first,
            offset,
            limit
        )
        return PagingUtils.buildPaginationResponse(
            serializeObject(compactedEntities),
            countAndEntities.first,
            count,
            prevAndNextLinks,
            mediaType, contextLink
        )
    }

    /**
     * Implements 6.5.3.1 - Retrieve Entity
     */
    @GetMapping("/{entityId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByURI(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val entityUri = entityId.toUri()
        val includeSysAttrs = params.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
            .contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
        val useSimplifiedRepresentation = params.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
            .contains(QUERY_PARAM_OPTIONS_KEYVALUES_VALUE)
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val sub = getSubFromSecurityContext()

        return either<APIException, ResponseEntity<*>> {
            entityService.checkExistence(entityUri).bind()
            authorizationService.isReadAuthorized(entityUri, entityService.getEntityType(entityUri), sub).bind()

            val jsonLdEntity = entityService.getFullEntityById(entityUri, includeSysAttrs)

            val expandedAttrs = parseAndExpandRequestParameter(params.getFirst("attrs"), contextLink)
            jsonLdEntity.checkContainsAnyOf(expandedAttrs).bind()
            val filteredJsonLdEntity = JsonLdEntity(
                filterJsonLdEntityOnAttributes(jsonLdEntity, expandedAttrs),
                jsonLdEntity.contexts
            )

            val compactedEntity = compact(filteredJsonLdEntity, contextLink, mediaType).toMutableMap()

            buildGetSuccessResponse(mediaType, contextLink)
                .let {
                    if (useSimplifiedRepresentation)
                        it.body(serializeObject(compactedEntity.toKeyValues()))
                    else
                        it.body(serializeObject(compactedEntity))
                }
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    /**
     * Implements 6.5.3.2 - Delete Entity
     */
    @DeleteMapping("/{entityId}")
    suspend fun delete(
        @PathVariable entityId: String
    ): ResponseEntity<*> {
        val entityUri = entityId.toUri()
        val sub = getSubFromSecurityContext()

        return either<APIException, ResponseEntity<*>> {
            entityService.checkExistence(entityUri).bind()
            // Is there a way to avoid loading the entity to get its type and contexts (for the event to be published)?
            val entity = entityService.getEntityCoreProperties(entityId.toUri())
            authorizationService.isAdminAuthorized(entityUri, entity.type[0], sub).bind()

            entityService.deleteEntity(entityUri)

            entityEventService.publishEntityDeleteEvent(sub.orNull(), entityId.toUri(), entity.type[0], entity.contexts)

            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    /**
     * Implements 6.6.3.1 - Append Entity Attributes
     *
     */
    @PostMapping("/{entityId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun appendEntityAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @RequestParam options: Optional<String>,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val entityUri = entityId.toUri()
        val disallowOverwrite = options.map { it == QUERY_PARAM_OPTIONS_NOOVERWRITE_VALUE }.orElse(false)

        return either<APIException, ResponseEntity<*>> {
            entityService.checkExistence(entityUri).bind()

            val body = requestBody.awaitFirst().deserializeAsMap()
            val contexts = checkAndGetContext(httpHeaders, body)
            val jsonLdAttributes = expandJsonLdFragment(body, contexts)
            val ngsiLdAttributes = parseToNgsiLdAttributes(jsonLdAttributes)
            authorizationService.isUpdateAuthorized(
                entityUri,
                entityService.getEntityType(entityUri),
                ngsiLdAttributes,
                sub
            ).bind()

            val updateResult = entityService.appendEntityAttributes(
                entityUri,
                ngsiLdAttributes,
                disallowOverwrite
            )

            if (updateResult.updated.isNotEmpty()) {
                entityEventService.publishAttributeAppendEvents(
                    sub.orNull(),
                    entityUri,
                    jsonLdAttributes,
                    updateResult,
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
    }

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
        @PathVariable entityId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val entityUri = entityId.toUri()

        return either<APIException, ResponseEntity<*>> {
            entityService.checkExistence(entityUri).bind()
            val body = requestBody.awaitFirst().deserializeAsMap()
            val contexts = checkAndGetContext(httpHeaders, body)
            val jsonLdAttributes = expandJsonLdFragment(body, contexts)
            val ngsiLdAttributes = parseToNgsiLdAttributes(jsonLdAttributes)
            authorizationService.isUpdateAuthorized(
                entityUri,
                entityService.getEntityType(entityUri),
                ngsiLdAttributes,
                sub
            ).bind()

            val updateResult = entityService.updateEntityAttributes(entityUri, ngsiLdAttributes)

            if (updateResult.updated.isNotEmpty()) {
                entityEventService.publishAttributeUpdateEvents(
                    sub.orNull(),
                    entityUri,
                    jsonLdAttributes,
                    updateResult,
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
    }

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
        @PathVariable entityId: String,
        @PathVariable attrId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val entityUri = entityId.toUri()

        return either<APIException, ResponseEntity<*>> {
            entityService.checkExistence(entityUri).bind()

            val body = mapOf(attrId to deserializeAs<Any>(requestBody.awaitFirst()))
            val contexts = checkAndGetContext(httpHeaders, body)
            val expandedAttrId = expandJsonLdTerm(attrId, contexts)!!
            authorizationService.isUpdateAuthorized(
                entityUri,
                entityService.getEntityType(entityUri),
                expandedAttrId,
                sub
            ).bind()

            val expandedPayload = expandJsonLdFragment(body, contexts)

            entityAttributeService.partialUpdateEntityAttribute(
                entityUri,
                expandedPayload as Map<String, List<Map<String, List<Any>>>>,
                contexts
            )
                .let {
                    if (it.updated.isEmpty())
                        ResourceNotFoundException("Unknown attribute in entity $entityId").left()
                    else {
                        entityEventService.publishPartialAttributeUpdateEvents(
                            sub.orNull(),
                            entityUri,
                            expandedPayload,
                            it.updated,
                            contexts
                        )

                        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>().right()
                    }.bind()
                }
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    /**
     * Implements 6.7.3.2 - Delete Entity Attribute
     */
    @DeleteMapping("/{entityId}/attrs/{attrId}")
    suspend fun deleteEntityAttribute(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @PathVariable attrId: String,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val entityUri = entityId.toUri()
        val deleteAll = params.getFirst("deleteAll")?.toBoolean() ?: false
        val datasetId = params.getFirst("datasetId")?.toUri()

        return either<APIException, ResponseEntity<*>> {
            entityService.checkExistence(entityUri).bind()

            val contexts = listOf(getContextFromLinkHeaderOrDefault(httpHeaders))
            val expandedAttrId = expandJsonLdTerm(attrId, contexts)!!
            authorizationService.isUpdateAuthorized(
                entityUri,
                entityService.getEntityType(entityUri),
                expandedAttrId,
                sub
            ).bind()

            val result = if (deleteAll)
                entityService.deleteEntityAttribute(entityUri, expandedAttrId)
            else
                entityService.deleteEntityAttributeInstance(entityUri, expandedAttrId, datasetId)

            if (result) {
                entityEventService.publishAttributeDeleteEvent(
                    sub.orNull(), entityUri, attrId, datasetId, deleteAll, contexts
                )
                ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
            } else
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).contentType(MediaType.APPLICATION_JSON)
                    .body(InternalErrorResponse("An error occurred while deleting $attrId from $entityId"))
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }
}
