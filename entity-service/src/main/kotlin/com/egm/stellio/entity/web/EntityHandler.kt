package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.config.ApplicationProperties
import com.egm.stellio.entity.service.EntityAttributeService
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonLdUtils.parseAndExpandAttributeFragment
import com.egm.stellio.shared.util.JsonLdUtils.reconstructPolygonCoordinates
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
import java.util.*

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
        if (!authorizationService.userCanCreateEntities(sub))
            throw AccessDeniedException("User forbidden to create entities")

        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)
        val ngsiLdEntity = expandJsonLdEntity(body, contexts).toNgsiLdEntity()
        val newEntityUri = entityService.createEntity(ngsiLdEntity)
        authorizationService.createAdminLink(newEntityUri, sub)

        entityEventService.publishEntityCreateEvent(
            sub.orNull(),
            ngsiLdEntity.id,
            ngsiLdEntity.type,
            contexts
        )
        return ResponseEntity
            .status(HttpStatus.CREATED)
            .location(URI("/ngsi-ld/v1/entities/$newEntityUri"))
            .build<String>()
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
        val ids = params.getFirst(QUERY_PARAM_ID)?.split(",")
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
            QueryParams(ids, type?.let { expandJsonLdKey(type, contextLink) }, idPattern, q?.decode(), expandedAttrs),
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
                        JsonLdUtils.filterJsonLdEntityOnAttributes(it, expandedAttrs),
                        it.contexts
                    )
                }

        val compactedEntities = compactEntities(filteredEntities, useSimplifiedRepresentation, contextLink, mediaType)
            .map { it.toMutableMap() }
        // coordinates of Polygon GeoProperty are returned in a single list after being compacted
        // so they should be reconstructed
        compactedEntities.forEach { reconstructPolygonCoordinates(it) }

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
    @Suppress("ThrowsCount")
    suspend fun getByURI(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val includeSysAttrs = params.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
            .contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
        val useSimplifiedRepresentation = params.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
            .contains(QUERY_PARAM_OPTIONS_KEYVALUES_VALUE)
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val sub = getSubFromSecurityContext()

        if (!entityService.exists(entityId.toUri()))
            throw ResourceNotFoundException(entityNotFoundMessage(entityId))
        if (!authorizationService.userCanReadEntity(entityId.toUri(), sub))
            throw AccessDeniedException("User forbidden read access to entity $entityId")

        val jsonLdEntity = entityService.getFullEntityById(entityId.toUri(), includeSysAttrs)

        val expandedAttrs = parseAndExpandRequestParameter(params.getFirst("attrs"), contextLink)
        if (jsonLdEntity.containsAnyOf(expandedAttrs)) {
            val filteredJsonLdEntity = JsonLdEntity(
                JsonLdUtils.filterJsonLdEntityOnAttributes(jsonLdEntity, expandedAttrs),
                jsonLdEntity.contexts
            )

            val compactedEntity = JsonLdUtils.compact(filteredJsonLdEntity, contextLink, mediaType).toMutableMap()

            // coordinates of Polygon GeoProperty are returned in a single list after being compacted
            // so they should be reconstructed
            reconstructPolygonCoordinates(compactedEntity)

            return buildGetSuccessResponse(mediaType, contextLink)
                .let {
                    if (useSimplifiedRepresentation)
                        it.body(serializeObject(compactedEntity.toKeyValues()))
                    else
                        it.body(serializeObject(compactedEntity))
                }
        } else
            throw ResourceNotFoundException("Entity $entityId does not have any of the requested attributes")
    }

    /**
     * Implements 6.5.3.2 - Delete Entity
     */
    @DeleteMapping("/{entityId}")
    suspend fun delete(
        @PathVariable entityId: String
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()

        if (!entityService.exists(entityId.toUri()))
            throw ResourceNotFoundException(entityNotFoundMessage(entityId))
        if (!authorizationService.userIsAdminOfEntity(entityId.toUri(), sub))
            throw AccessDeniedException("User forbidden admin access to entity $entityId")

        // Is there a way to avoid loading the entity to get its type and contexts (for the event to be published)?
        val entity = entityService.getEntityCoreProperties(entityId.toUri())

        entityService.deleteEntity(entityId.toUri())

        entityEventService.publishEntityDeleteEvent(sub.orNull(), entityId.toUri(), entity.type[0], entity.contexts)

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
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
        val disallowOverwrite = options.map { it == QUERY_PARAM_OPTIONS_NOOVERWRITE_VALUE }.orElse(false)
        val entityUri = entityId.toUri()

        if (!entityService.exists(entityUri))
            throw ResourceNotFoundException("Entity $entityId does not exist")

        val sub = getSubFromSecurityContext()
        if (!authorizationService.userCanUpdateEntity(entityUri, sub))
            throw AccessDeniedException("User forbidden write access to entity $entityId")

        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = expandJsonLdFragment(body, contexts)
        val ngsiLdAttributes = parseToNgsiLdAttributes(jsonLdAttributes)
        authorizationService.checkAttributesAreAuthorized(ngsiLdAttributes, entityUri)

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

        return if (updateResult.notUpdated.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(updateResult)
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
        val entityUri = entityId.toUri()

        if (!entityService.exists(entityUri))
            throw ResourceNotFoundException("Entity $entityId does not exist")

        val sub = getSubFromSecurityContext()
        if (!authorizationService.userCanUpdateEntity(entityUri, sub))
            throw AccessDeniedException("User forbidden write access to entity $entityId")

        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = expandJsonLdFragment(body, contexts)
        val ngsiLdAttributes = parseToNgsiLdAttributes(jsonLdAttributes)
        authorizationService.checkAttributesAreAuthorized(ngsiLdAttributes, entityUri)
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

        return if (updateResult.notUpdated.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(updateResult)
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

        if (!entityService.exists(entityUri))
            throw ResourceNotFoundException("Entity $entityId does not exist")
        if (!authorizationService.userCanUpdateEntity(entityUri, sub))
            throw AccessDeniedException("User forbidden write access to entity $entityId")

        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)

        val expandedAttrId = expandJsonLdKey(attrId, contexts)!!
        authorizationService.checkAttributeIsAuthorized(expandedAttrId, entityUri)

        val expandedPayload = parseAndExpandAttributeFragment(attrId, body, contexts)

        val updateResult = entityAttributeService.partialUpdateEntityAttribute(entityUri, expandedPayload, contexts)

        if (updateResult.updated.isEmpty())
            throw ResourceNotFoundException("Unknown attribute in entity $entityId")
        else
            entityEventService.publishPartialAttributeUpdateEvents(
                sub.orNull(),
                entityUri,
                expandedPayload,
                updateResult.updated,
                contexts
            )

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
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
        val entityUri = entityId.toUri()
        val deleteAll = params.getFirst("deleteAll")?.toBoolean() ?: false
        val datasetId = params.getFirst("datasetId")?.toUri()
        val sub = getSubFromSecurityContext()

        if (!entityService.exists(entityUri))
            throw ResourceNotFoundException("Entity $entityId does not exist")
        if (!authorizationService.userCanUpdateEntity(entityUri, sub))
            throw AccessDeniedException("User forbidden write access to entity $entityId")

        val contexts = listOf(getContextFromLinkHeaderOrDefault(httpHeaders))
        val expandedAttrId = expandJsonLdKey(attrId, contexts)!!
        authorizationService.checkAttributeIsAuthorized(expandedAttrId, entityUri)

        val result = if (deleteAll)
            entityService.deleteEntityAttribute(entityUri, expandedAttrId)
        else
            entityService.deleteEntityAttributeInstance(entityUri, expandedAttrId, datasetId)

        if (result)
            entityEventService.publishAttributeDeleteEvent(
                sub.orNull(), entityUri, attrId, datasetId, deleteAll, contexts
            )

        return if (result)
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).contentType(MediaType.APPLICATION_JSON)
                .body(InternalErrorResponse("An error occurred while deleting $attrId from $entityId"))
    }
}
