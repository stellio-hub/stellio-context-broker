package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.entity.util.decode
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.compactAndStringifyFragment
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.web.extractSubjectOrEmpty
import kotlinx.coroutines.reactive.awaitFirst
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
import java.util.Optional

@RestController
@RequestMapping("/ngsi-ld/v1/entities")
class EntityHandler(
    private val entityService: EntityService,
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
        val userId = extractSubjectOrEmpty().awaitFirst()
        if (!authorizationService.userCanCreateEntities(userId))
            throw AccessDeniedException("User forbidden to create entities")

        val body = requestBody.awaitFirst()
        val ngsiLdEntity = expandJsonLdEntity(body, checkAndGetContext(httpHeaders, body)).toNgsiLdEntity()
        val newEntityUri = entityService.createEntity(ngsiLdEntity)
        authorizationService.createAdminLink(newEntityUri, userId)

        entityEventService.publishEntityEvent(
            EntityCreateEvent(newEntityUri, body),
            ngsiLdEntity.type.extractShortTypeFromExpanded()
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
        val type = params.getFirst(QUERY_PARAM_TYPE) ?: ""
        val q = params.getOrDefault(QUERY_PARAM_FILTER, emptyList())
        val includeSysAttrs = params.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
            .contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)

        // TODO 6.4.3.2 says that either type or attrs must be provided (and not type or q)
        if (q.isNullOrEmpty() && type.isEmpty())
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(
                    BadRequestDataResponse(
                        "'q' or 'type' request parameters have to be specified (TEMP - cf 6.4.3.2)"
                    )
                )

        if (!JsonLdUtils.isTypeResolvable(type, contextLink))
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(BadRequestDataResponse("Unable to resolve 'type' parameter from the provided Link header"))

        /**
         * Decoding query parameters is not supported by default so a call to a decode function was added query
         * with the right parameters values
         */
        val entities = entityService.searchEntities(type, q.decode(), contextLink, includeSysAttrs)
        val userId = extractSubjectOrEmpty().awaitFirst()
        val entitiesUserCanRead =
            authorizationService.filterEntitiesUserCanRead(
                entities.map { it.id.toUri() },
                userId
            ).toListOfString()
        val filteredEntities = entities.filter { entitiesUserCanRead.contains(it.id) }
        val compactedEntities = compactEntities(filteredEntities)

        return ResponseEntity.status(HttpStatus.OK).body(serializeObject(compactedEntities))
    }

    /**
     * Implements 6.5.3.1 - Retrieve Entity
     */
    @GetMapping("/{entityId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    @Suppress("ThrowsCount")
    suspend fun getByURI(
        @PathVariable entityId: String,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val includeSysAttrs = params.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
            .contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
        val userId = extractSubjectOrEmpty().awaitFirst()

        if (!entityService.exists(entityId.toUri()))
            throw ResourceNotFoundException(entityNotFoundMessage(entityId))
        if (!authorizationService.userCanReadEntity(entityId.toUri(), userId))
            throw AccessDeniedException("User forbidden read access to entity $entityId")

        val entity = entityService.getFullEntityById(entityId.toUri(), includeSysAttrs)
            ?: throw ResourceNotFoundException(entityNotFoundMessage(entityId))

        return ResponseEntity.status(HttpStatus.OK).body(serializeObject(entity.compact()))
    }

    /**
     * Implements 6.5.3.2 - Delete Entity
     */
    @DeleteMapping("/{entityId}")
    suspend fun delete(@PathVariable entityId: String): ResponseEntity<*> {
        val userId = extractSubjectOrEmpty().awaitFirst()

        if (!entityService.exists(entityId.toUri()))
            throw ResourceNotFoundException(entityNotFoundMessage(entityId))
        if (!authorizationService.userIsAdminOfEntity(entityId.toUri(), userId))
            throw AccessDeniedException("User forbidden admin access to entity $entityId")

        val entityType = entityService.getEntityType(entityId.toUri())
        entityService.deleteEntity(entityId.toUri())

        entityEventService.publishEntityEvent(
            EntityDeleteEvent(entityId.toUri()), entityType.extractShortTypeFromExpanded()
        )

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
        val disallowOverwrite = options.map { it == "noOverwrite" }.orElse(false)

        if (!entityService.exists(entityId.toUri()))
            throw ResourceNotFoundException("Entity $entityId does not exist")

        val userId = extractSubjectOrEmpty().awaitFirst()
        if (!authorizationService.userCanUpdateEntity(entityId.toUri(), userId))
            throw AccessDeniedException("User forbidden write access to entity $entityId")

        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = expandJsonLdFragment(body, contexts)
        val updateResult = entityService.appendEntityAttributes(
            entityId.toUri(),
            parseToNgsiLdAttributes(jsonLdAttributes),
            disallowOverwrite
        )

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
        if (!entityService.exists(entityId.toUri()))
            throw ResourceNotFoundException("Entity $entityId does not exist")

        val userId = extractSubjectOrEmpty().awaitFirst()
        if (!authorizationService.userCanUpdateEntity(entityId.toUri(), userId))
            throw AccessDeniedException("User forbidden write access to entity $entityId")

        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = expandJsonLdFragment(body, contexts)
        val ngsiLdAttributes = parseToNgsiLdAttributes(jsonLdAttributes)
        val updateResult =
            entityService.updateEntityAttributes(entityId.toUri(), ngsiLdAttributes)

        val updatedEntity = entityService.getFullEntityById(entityId.toUri(), true)

        updateResult.updated.forEach { expandedAttributeName ->
            entityEventService.publishEntityEvent(
                AttributeReplaceEvent(
                    entityId.toUri(),
                    expandedAttributeName.extractShortTypeFromExpanded(),
                    extractDatasetIdFromNgsiLdAttributes(ngsiLdAttributes, expandedAttributeName),
                    compactAndStringifyFragment(
                        expandedAttributeName,
                        jsonLdAttributes[expandedAttributeName]!!,
                        contexts
                    ),
                    JsonLdUtils.compactAndSerialize(updatedEntity!!),
                    contexts
                ),
                updatedEntity.type.extractShortTypeFromExpanded()
            )
        }

        return if (updateResult.notUpdated.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(updateResult)
    }

    /**
     * Implements 6.7.3.1 - Partial Attribute Update
     * Current implementation is basic and only update the value of a property.
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
        val userId = extractSubjectOrEmpty().awaitFirst()

        if (!entityService.exists(entityId.toUri()))
            throw ResourceNotFoundException("Entity $entityId does not exist")
        if (!authorizationService.userCanUpdateEntity(entityId.toUri(), userId))
            throw AccessDeniedException("User forbidden write access to entity $entityId")

        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)

        entityService.updateEntityAttribute(entityId.toUri(), attrId, body, contexts)

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
        val deleteAll = params.getFirst("deleteAll")?.toBoolean() ?: false
        val datasetId = params.getFirst("datasetId")?.toUri()
        val userId = extractSubjectOrEmpty().awaitFirst()

        if (!entityService.exists(entityId.toUri()))
            throw ResourceNotFoundException("Entity $entityId does not exist")
        if (!authorizationService.userCanUpdateEntity(entityId.toUri(), userId))
            throw AccessDeniedException("User forbidden write access to entity $entityId")

        val contexts = listOf(getContextFromLinkHeaderOrDefault(httpHeaders))
        val result = if (deleteAll)
            entityService.deleteEntityAttribute(entityId.toUri(), expandJsonLdKey(attrId, contexts)!!)
        else
            entityService.deleteEntityAttributeInstance(
                entityId.toUri(), expandJsonLdKey(attrId, contexts)!!, datasetId
            )

        return if (result)
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).contentType(MediaType.APPLICATION_JSON)
                .body(InternalErrorResponse("An error occurred while deleting $attrId from $entityId"))
    }
}
