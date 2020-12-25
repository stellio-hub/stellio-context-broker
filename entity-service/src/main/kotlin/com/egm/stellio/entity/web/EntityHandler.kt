package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.service.EntityAttributeService
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.entity.util.decode
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.compactAndSerialize
import com.egm.stellio.shared.util.JsonLdUtils.compactAndStringifyFragment
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsString
import com.egm.stellio.shared.util.JsonLdUtils.removeContextFromInput
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
        val userId = extractSubjectOrEmpty().awaitFirst()
        if (!authorizationService.userCanCreateEntities(userId))
            throw AccessDeniedException("User forbidden to create entities")

        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)
        val ngsiLdEntity = expandJsonLdEntity(body, contexts).toNgsiLdEntity()
        val newEntityUri = entityService.createEntity(ngsiLdEntity)
        authorizationService.createAdminLink(newEntityUri, userId)

        entityEventService.publishEntityEvent(
            EntityCreateEvent(newEntityUri, removeContextFromInput(body), contexts),
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
        val id = params.getFirst(QUERY_PARAM_ID)
        val type = params.getFirst(QUERY_PARAM_TYPE) ?: ""
        val q = params.getFirst(QUERY_PARAM_FILTER) ?: ""
        val includeSysAttrs = params.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
            .contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
        val useSimplifiedRepresentation = params.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
            .contains(QUERY_PARAM_OPTIONS_KEYVALUES_VALUE)
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)

        // TODO 6.4.3.2 says that either type or attrs must be provided (and not type or q)
        if (q.isEmpty() && type.isEmpty())
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
        val entities = entityService.searchEntities(id, type, q.decode(), contextLink, includeSysAttrs)
        val userId = extractSubjectOrEmpty().awaitFirst()
        val entitiesUserCanRead =
            authorizationService.filterEntitiesUserCanRead(
                entities.map { it.id.toUri() },
                userId
            ).toListOfString()

        val expandedAttrs = parseAndExpandAttrsParameter(params.getFirst("attrs"), contextLink)
        val filteredEntities =
            entities.filter { entitiesUserCanRead.contains(it.id) }
                .filter { it.containsAnyOf(expandedAttrs) }
                .map {
                    JsonLdEntity(
                        JsonLdUtils.filterJsonLdEntityOnAttributes(it, expandedAttrs),
                        it.contexts
                    )
                }

        val compactedEntities = compactEntities(filteredEntities, useSimplifiedRepresentation, contextLink, mediaType)

        return buildGetSuccessResponse(mediaType, contextLink)
            .body(serializeObject(compactedEntities))
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
        val userId = extractSubjectOrEmpty().awaitFirst()

        if (!entityService.exists(entityId.toUri()))
            throw ResourceNotFoundException(entityNotFoundMessage(entityId))
        if (!authorizationService.userCanReadEntity(entityId.toUri(), userId))
            throw AccessDeniedException("User forbidden read access to entity $entityId")

        val jsonLdEntity = entityService.getFullEntityById(entityId.toUri(), includeSysAttrs)
            ?: throw ResourceNotFoundException(entityNotFoundMessage(entityId))

        val expandedAttrs = parseAndExpandAttrsParameter(params.getFirst("attrs"), contextLink)
        if (jsonLdEntity.containsAnyOf(expandedAttrs)) {
            val filteredJsonLdEntity = JsonLdEntity(
                JsonLdUtils.filterJsonLdEntityOnAttributes(jsonLdEntity, expandedAttrs),
                jsonLdEntity.contexts
            )
            val compactedEntity = JsonLdUtils.compact(filteredJsonLdEntity, contextLink, mediaType)

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
    suspend fun delete(@PathVariable entityId: String): ResponseEntity<*> {
        val userId = extractSubjectOrEmpty().awaitFirst()

        if (!entityService.exists(entityId.toUri()))
            throw ResourceNotFoundException(entityNotFoundMessage(entityId))
        if (!authorizationService.userIsAdminOfEntity(entityId.toUri(), userId))
            throw AccessDeniedException("User forbidden admin access to entity $entityId")

        val entity = entityService.getEntityCoreProperties(entityId.toUri())

        entityService.deleteEntity(entityId.toUri())

        // FIXME The context is not supposed to be retrieved from DB
        entityEventService.publishEntityEvent(
            EntityDeleteEvent(entityId.toUri(), entity.contexts), entity.type[0].extractShortTypeFromExpanded()
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

        if (updateResult.updated.isNotEmpty()) {
            entityEventService.publishAppendEntityAttributesEvents(
                entityId.toUri(),
                jsonLdAttributes,
                updateResult,
                entityService.getFullEntityById(entityId.toUri(), true)!!,
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

        updateResult.updated.forEach { updatedDetails ->
            entityEventService.publishEntityEvent(
                AttributeReplaceEvent(
                    entityId.toUri(),
                    updatedDetails.attributeName.extractShortTypeFromExpanded(),
                    extractDatasetIdFromNgsiLdAttributes(ngsiLdAttributes, updatedDetails.attributeName),
                    compactAndStringifyFragment(
                        updatedDetails.attributeName,
                        jsonLdAttributes[updatedDetails.attributeName]!!,
                        contexts
                    ),
                    compactAndSerialize(updatedEntity!!, contexts, MediaType.APPLICATION_JSON),
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
        val userId = extractSubjectOrEmpty().awaitFirst()
        val entityUri = entityId.toUri()

        if (!entityService.exists(entityUri))
            throw ResourceNotFoundException("Entity $entityId does not exist")
        if (!authorizationService.userCanUpdateEntity(entityUri, userId))
            throw AccessDeniedException("User forbidden write access to entity $entityId")

        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)

        val expandedBody = expandJsonLdFragment(body, contexts) as Map<String, List<Any>>
        val expandedAttrId = expandJsonLdKey(attrId, contexts)!!
        val expandedPayload = mapOf(expandedAttrId to expandedBody)
        entityAttributeService.partialUpdateEntityAttribute(entityUri, expandedPayload, contexts)

        val updatedEntity = entityService.getFullEntityById(entityId.toUri(), true)
        entityEventService.publishEntityEvent(
            AttributeUpdateEvent(
                entityId = entityUri,
                attributeName = attrId,
                datasetId = getPropertyValueFromMapAsString(expandedBody, NGSILD_DATASET_ID_PROPERTY)?.toUri(),
                operationPayload = removeContextFromInput(body),
                updatedEntity = compactAndSerialize(updatedEntity!!, contexts, MediaType.APPLICATION_JSON),
                contexts = contexts
            ),
            updatedEntity.type.extractShortTypeFromExpanded()
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

        if (result) {
            val updatedEntity = entityService.getFullEntityById(entityId.toUri(), true)
            if (deleteAll)
                entityEventService.publishEntityEvent(
                    AttributeDeleteAllInstancesEvent(
                        entityId = entityId.toUri(),
                        attributeName = attrId,
                        updatedEntity = compactAndSerialize(updatedEntity!!, contexts, MediaType.APPLICATION_JSON),
                        contexts = contexts
                    ),
                    updatedEntity.type.extractShortTypeFromExpanded()
                )
            else
                entityEventService.publishEntityEvent(
                    AttributeDeleteEvent(
                        entityId = entityId.toUri(),
                        attributeName = attrId,
                        datasetId = datasetId,
                        updatedEntity = compactAndSerialize(updatedEntity!!, contexts, MediaType.APPLICATION_JSON),
                        contexts = contexts
                    ),
                    updatedEntity.type.extractShortTypeFromExpanded()
                )
        }

        return if (result)
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).contentType(MediaType.APPLICATION_JSON)
                .body(InternalErrorResponse("An error occurred while deleting $attrId from $entityId"))
    }
}
