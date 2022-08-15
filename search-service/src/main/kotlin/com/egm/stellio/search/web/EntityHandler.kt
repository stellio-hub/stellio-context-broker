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
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
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
    private val entityPayloadService: EntityPayloadService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val queryService: QueryService,
    private val authorizationService: AuthorizationService,
    private val entityAccessRightsService: EntityAccessRightsService,
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
        val jsonLdEntity = expandJsonLdEntity(body, contexts)
        val ngsiLdEntity = jsonLdEntity.toNgsiLdEntity()

        return either<APIException, ResponseEntity<*>> {
            authorizationService.checkCreationAuthorized(ngsiLdEntity, sub).bind()
            entityPayloadService.checkEntityExistence(ngsiLdEntity.id, true).bind()
            temporalEntityAttributeService.createEntityTemporalReferences(
                ngsiLdEntity, jsonLdEntity, sub.orNull()
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

//    /**
//     * Implements 6.4.3.2 - Query Entities
//     */
//    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
//    suspend fun getEntities(
//        @RequestHeader httpHeaders: HttpHeaders,
//        @RequestParam params: MultiValueMap<String, String>
//    ): ResponseEntity<*> {
//        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
//        val mediaType = getApplicableMediaType(httpHeaders)
//        val sub = getSubFromSecurityContext()
//
//        val queryParams = parseAndCheckParams(
//            Pair(applicationProperties.pagination.limitDefault, applicationProperties.pagination.limitMax),
//            params,
//            contextLink
//        )
//
//        if (
//            queryParams.ids.isEmpty() &&
//            queryParams.q.isNullOrEmpty() &&
//            queryParams.types.isEmpty() &&
//            queryParams.attrs.isEmpty()
//        )
//            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
//                .body(
//                    BadRequestDataResponse(
//                        "one of 'id', 'q', 'type' and 'attrs' request parameters have to be specified"
//                    )
//                )
//
//        val countAndEntities = entityService.searchEntities(queryParams, sub, contextLink)
//
//        val filteredEntities =
//            countAndEntities.second.filter { it.containsAnyOf(queryParams.attrs) }
//                .map {
//                    JsonLdEntity(
//                        JsonLdUtils.filterJsonLdEntityOnAttributes(it, queryParams.attrs),
//                        it.contexts
//                    )
//                }
//
//        val compactedEntities = JsonLdUtils.compactEntities(
//            filteredEntities,
//            queryParams.useSimplifiedRepresentation,
//            contextLink,
//            mediaType
//        )
//
//        return buildQueryResponse(
//            compactedEntities,
//            countAndEntities.first,
//            "/ngsi-ld/v1/entities",
//            queryParams,
//            params,
//            mediaType,
//            contextLink
//        )
//    }

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

            val entityTypes = entityPayloadService.getTypes(entityUri).bind()
            authorizationService.checkReadAuthorized(entityUri, entityTypes, sub).bind()

            val jsonLdEntity = queryService.queryEntity(entityUri, listOf(contextLink)).bind()

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
            authorizationService.checkAdminAuthorized(entityUri, entity.types, sub).bind()

            temporalEntityAttributeService.deleteTemporalEntityReferences(entityUri).bind()

            entityAccessRightsService.removeRolesOnEntity(entityUri).bind()

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
            entityPayloadService.checkEntityExistence(entityUri).bind()

            val body = requestBody.awaitFirst().deserializeAsMap()
            val contexts = checkAndGetContext(httpHeaders, body)
            val jsonLdAttributes = expandJsonLdFragment(body, contexts)
            val (typeAttr, otherAttrs) = jsonLdAttributes.toList().partition { it.first == JsonLdUtils.JSONLD_TYPE }
            val ngsiLdAttributes = parseToNgsiLdAttributes(otherAttrs.toMap())

            val entityTypes = entityPayloadService.getTypes(entityUri).bind()
            authorizationService.checkUpdateAuthorized(entityUri, entityTypes, ngsiLdAttributes, sub).bind()

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

            val entityTypes = entityPayloadService.getTypes(entityUri).bind()
            authorizationService.checkUpdateAuthorized(entityUri, entityTypes, ngsiLdAttributes, sub).bind()

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

            val entityTypes = entityPayloadService.getTypes(entityUri).bind()
            authorizationService.checkUpdateAuthorized(entityUri, entityTypes, expandedAttrId, sub).bind()

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

            val entityTypes = entityPayloadService.getTypes(entityUri).bind()
            authorizationService.checkUpdateAuthorized(entityUri, entityTypes, expandedAttrId, sub).bind()

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
}
