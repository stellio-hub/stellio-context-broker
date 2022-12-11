package com.egm.stellio.search.web

import arrow.core.continuations.either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.authorization.EntityAccessRightsService
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.service.EntityEventService
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.util.prepareTemporalAttributes
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.LoggerFactory
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
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val queryService: QueryService,
    private val authorizationService: AuthorizationService,
    private val entityAccessRightsService: EntityAccessRightsService,
    private val entityEventService: EntityEventService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

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
        val jsonLdEntity = expandJsonLdEntity(body, contexts)
        val ngsiLdEntity = jsonLdEntity.toNgsiLdEntity()

        return either<APIException, ResponseEntity<*>> {
            val attributesMetadata = ngsiLdEntity.prepareTemporalAttributes().bind()
            authorizationService.userCanCreateEntities(sub).bind()
            entityPayloadService.checkEntityExistence(ngsiLdEntity.id, true).bind()
            temporalEntityAttributeService.createEntityTemporalReferences(
                ngsiLdEntity, jsonLdEntity, attributesMetadata, sub.orNull()
            ).bind()
            authorizationService.createAdminLink(ngsiLdEntity.id, sub).bind()

            entityEventService.publishEntityCreateEvent(
                sub.orNull(),
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

        return either<APIException, ResponseEntity<*>> {
            val queryParams = parseAndCheckParams(
                Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
                params,
                contextLink
            )

            if (
                queryParams.ids.isEmpty() &&
                queryParams.q.isNullOrEmpty() &&
                queryParams.types.isEmpty() &&
                queryParams.attrs.isEmpty()
            )
                BadRequestDataException(
                    "one of 'id', 'q', 'type' and 'attrs' request parameters have to be specified"
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
            )

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
            entityPayloadService.checkEntityExistence(entityUri).bind()

            authorizationService.userCanReadEntity(entityUri, sub).bind()

            val jsonLdEntity =
                queryService.queryEntity(entityUri, listOf(contextLink), queryParams.includeSysAttrs).bind()

            jsonLdEntity.checkContainsAnyOf(queryParams.attrs).bind()

            val filteredJsonLdEntity = JsonLdEntity(
                JsonLdUtils.filterJsonLdEntityOnAttributes(jsonLdEntity, queryParams.attrs),
                jsonLdEntity.contexts
            )
            val compactedEntity = JsonLdUtils.compact(filteredJsonLdEntity, contextLink, mediaType).toMutableMap()

            prepareGetSuccessResponse(mediaType, contextLink)
                .let {
                    if (queryParams.useSimplifiedRepresentation)
                        it.body(JsonUtils.serializeObject(compactedEntity.toKeyValues()))
                    else
                        it.body(JsonUtils.serializeObject(compactedEntity))
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
            entityPayloadService.checkEntityExistence(entityUri).bind()
            // Is there a way to avoid loading the entity to get its type and contexts (for the event to be published)?
            val entity = entityPayloadService.retrieve(entityId.toUri()).bind()
            authorizationService.userIsAdminOfEntity(entityUri, sub).bind()

            temporalEntityAttributeService.deleteTemporalEntityReferences(entityUri).bind()

            entityAccessRightsService.removeRolesOnEntity(entityUri).bind()

            entityEventService.publishEntityDeleteEvent(sub.orNull(), entityId.toUri(), entity.types, entity.contexts)

            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    @DeleteMapping("/", "")
    suspend fun handleMissingEntityIdOnDelete(): ResponseEntity<*> =
        missingPathErrorResponse("Missing entity id when trying to delete an entity")

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
            entityPayloadService.checkEntityExistence(entityUri).bind()

            val body = requestBody.awaitFirst().deserializeAsMap()
            val contexts = checkAndGetContext(httpHeaders, body)
            val jsonLdAttributes = expandJsonLdFragment(body, contexts)
            val (typeAttr, otherAttrs) = jsonLdAttributes.toList().partition { it.first == JsonLdUtils.JSONLD_TYPE }
            val ngsiLdAttributes = parseToNgsiLdAttributes(otherAttrs.toMap())

            authorizationService.userCanUpdateEntity(entityUri, sub).bind()

            val updateResult = entityPayloadService.updateTypes(
                entityUri,
                typeAttr.map { it.second as List<ExpandedTerm> }.firstOrNull().orEmpty()
            ).bind().mergeWith(
                temporalEntityAttributeService.appendEntityAttributes(
                    entityUri,
                    ngsiLdAttributes,
                    jsonLdAttributes,
                    disallowOverwrite,
                    sub.orNull()
                ).bind()
            )

            if (updateResult.updated.isNotEmpty()) {
                entityEventService.publishAttributeChangeEvents(
                    sub.orNull(),
                    entityUri,
                    jsonLdAttributes,
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
    }

    @PostMapping("/attrs")
    suspend fun handleMissingEntityIdOnAttributeAppend(): ResponseEntity<*> =
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
        @PathVariable entityId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val entityUri = entityId.toUri()
        val body = requestBody.awaitFirst().deserializeAsMap()
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = expandJsonLdFragment(body, contexts)
        val (typeAttr, otherAttrs) = jsonLdAttributes.toList().partition { it.first == JsonLdUtils.JSONLD_TYPE }
        val ngsiLdAttributes = parseToNgsiLdAttributes(otherAttrs.toMap())

        return either<APIException, ResponseEntity<*>> {
            entityPayloadService.checkEntityExistence(entityUri).bind()

            authorizationService.userCanUpdateEntity(entityUri, sub).bind()

            val updateResult = entityPayloadService.updateTypes(
                entityUri,
                typeAttr.map { it.second as List<ExpandedTerm> }.firstOrNull().orEmpty()
            ).bind().mergeWith(
                temporalEntityAttributeService.updateEntityAttributes(
                    entityUri,
                    ngsiLdAttributes,
                    jsonLdAttributes,
                    sub.orNull()
                ).bind()
            )

            if (updateResult.updated.isNotEmpty()) {
                entityEventService.publishAttributeChangeEvents(
                    sub.orNull(),
                    entityUri,
                    jsonLdAttributes,
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
            entityPayloadService.checkEntityExistence(entityUri).bind()

            val body = mapOf(attrId to JsonUtils.deserializeAs<Any>(requestBody.awaitFirst()))
            val contexts = checkAndGetContext(httpHeaders, body)
            val expandedPayload = expandJsonLdFragment(body, contexts)
            val expandedAttrId = expandedPayload.keys.first()

            authorizationService.userCanUpdateEntity(entityUri, sub).bind()

            when (expandedAttrId) {
                JsonLdUtils.JSONLD_TYPE ->
                    entityPayloadService.updateTypes(
                        entityUri,
                        expandedPayload[JsonLdUtils.JSONLD_TYPE] as List<ExpandedTerm>,
                        false
                    ).bind()
                else ->
                    temporalEntityAttributeService.partialUpdateEntityAttribute(
                        entityUri,
                        expandedPayload as Map<String, List<Map<String, List<Any>>>>,
                        sub.orNull()
                    ).bind()
            }.let {
                if (it.updated.isEmpty())
                    ResourceNotFoundException("Unknown attribute in entity $entityId").left()
                else {
                    entityEventService.publishAttributeChangeEvents(
                        sub.orNull(),
                        entityUri,
                        expandedPayload,
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
            val contexts = listOf(getContextFromLinkHeaderOrDefault(httpHeaders))
            val expandedAttrId = JsonLdUtils.expandJsonLdTerm(attrId, contexts)

            temporalEntityAttributeService.checkEntityAndAttributeExistence(
                entityUri, expandedAttrId, datasetId
            ).bind()

            authorizationService.userCanUpdateEntity(entityUri, sub).bind()

            temporalEntityAttributeService.deleteTemporalAttribute(
                entityUri, expandedAttrId, datasetId, deleteAll
            ).bind()

            entityEventService.publishAttributeDeleteEvent(
                sub.orNull(), entityUri, attrId, datasetId, deleteAll, contexts
            )
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    @DeleteMapping("/attrs/{attrId}", "/{entityId}/attrs")
    suspend fun handleMissingEntityIdOrAttributeOnDeleteAttribute(): ResponseEntity<*> =
        missingPathErrorResponse("Missing entity id or attribute id when trying to delete an attribute")

    private suspend fun missingPathErrorResponse(errorMessage: String): ResponseEntity<*> {
        logger.error("Bad Request: $errorMessage")
        return BadRequestDataException(errorMessage).toErrorResponse()
    }
}
