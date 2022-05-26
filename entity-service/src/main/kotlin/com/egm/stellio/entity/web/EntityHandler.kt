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
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.compact
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonLdUtils.filterJsonLdEntityOnAttributes
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.withContext
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
                ngsiLdEntity.types,
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
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val sub = getSubFromSecurityContext()

        val queryParams = parseAndCheckParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        )

        if (queryParams.q.isNullOrEmpty() && queryParams.types.isNullOrEmpty() && queryParams.attrs.isNullOrEmpty())
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(
                    BadRequestDataResponse(
                        "one of 'q', 'type' and 'attrs' request parameters have to be specified"
                    )
                )

        val countAndEntities = entityService.searchEntities(queryParams, sub, contextLink)

        val filteredEntities =
            countAndEntities.second.filter { it.containsAnyOf(queryParams.attrs) }
                .map {
                    JsonLdEntity(
                        filterJsonLdEntityOnAttributes(it, queryParams.attrs),
                        it.contexts
                    )
                }

        val compactedEntities = compactEntities(
            filteredEntities,
            queryParams.useSimplifiedRepresentation,
            contextLink,
            mediaType
        )

        return buildQueryResponse(
            compactedEntities,
            countAndEntities.first,
            "/ngsi-ld/v1/entities",
            queryParams,
            params,
            mediaType,
            contextLink
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
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val sub = getSubFromSecurityContext()

        val queryParams = parseAndCheckParams(
            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
            params,
            contextLink
        )

        return either<APIException, ResponseEntity<*>> {
            entityService.checkExistence(entityUri).bind()
            authorizationService.isReadAuthorized(entityUri, entityService.getEntityTypes(entityUri), sub).bind()

            val jsonLdEntity = entityService.getFullEntityById(entityUri, queryParams.includeSysAttrs)

            jsonLdEntity.checkContainsAnyOf(queryParams.attrs).bind()

            val filteredJsonLdEntity = JsonLdEntity(
                filterJsonLdEntityOnAttributes(jsonLdEntity, queryParams.attrs),
                jsonLdEntity.contexts
            )
            val compactedEntity = compact(filteredJsonLdEntity, contextLink, mediaType).toMutableMap()

            prepareGetSuccessResponse(mediaType, contextLink)
                .let {
                    if (queryParams.useSimplifiedRepresentation)
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
            authorizationService.isAdminAuthorized(entityUri, entity.types, sub).bind()

            entityService.deleteEntity(entityUri)

            entityEventService.publishEntityDeleteEvent(sub.orNull(), entityId.toUri(), entity.types, entity.contexts)

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
            val (typeAttr, otherAttrs) = jsonLdAttributes.toList().partition { it.first == JSONLD_TYPE }
            val ngsiLdAttributes = parseToNgsiLdAttributes(otherAttrs.toMap())
            authorizationService.isUpdateAuthorized(
                entityUri,
                entityService.getEntityTypes(entityUri),
                ngsiLdAttributes,
                sub
            ).bind()

            val updateResult = withContext(Dispatchers.IO) {
                entityService.appendEntityTypes(
                    entityUri,
                    typeAttr.map { it.second as List<ExpandedTerm> }.firstOrNull().orEmpty()
                ).mergeWith(
                    entityService.appendEntityAttributes(
                        entityUri,
                        ngsiLdAttributes,
                        disallowOverwrite
                    )
                )
            }

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
            val (typeAttr, otherAttrs) = jsonLdAttributes.toList().partition { it.first == JSONLD_TYPE }
            val ngsiLdAttributes = parseToNgsiLdAttributes(otherAttrs.toMap())
            authorizationService.isUpdateAuthorized(
                entityUri,
                entityService.getEntityTypes(entityUri),
                ngsiLdAttributes,
                sub
            ).bind()

            val updateResult = withContext(Dispatchers.IO) {
                entityService.appendEntityTypes(
                    entityUri,
                    typeAttr.map { it.second as List<ExpandedTerm> }.firstOrNull().orEmpty()
                ).mergeWith(
                    entityService.updateEntityAttributes(entityUri, ngsiLdAttributes)
                )
            }

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
            val expandedPayload = expandJsonLdFragment(body, contexts)
            val expandedAttrId = expandedPayload.keys.first()

            authorizationService.isUpdateAuthorized(
                entityUri,
                entityService.getEntityTypes(entityUri),
                expandedAttrId,
                sub
            ).bind()

            withContext(Dispatchers.IO) {
                when (expandedAttrId) {
                    JSONLD_TYPE ->
                        entityService.appendEntityTypes(
                            entityUri,
                            expandedPayload[JSONLD_TYPE] as List<ExpandedTerm>
                        )
                    else ->
                        entityAttributeService.partialUpdateEntityAttribute(
                            entityUri,
                            expandedPayload as Map<String, List<Map<String, List<Any>>>>,
                            contexts
                        )
                }
            }.let {
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
            val expandedAttrId = expandJsonLdTerm(attrId, contexts)
            authorizationService.isUpdateAuthorized(
                entityUri,
                entityService.getEntityTypes(entityUri),
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
