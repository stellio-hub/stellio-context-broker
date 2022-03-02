package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntities
import com.egm.stellio.shared.util.JsonLdUtils.extractContextFromInput
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.getSubFromSecurityContext
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
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
@RequestMapping("/ngsi-ld/v1/entityOperations")
class EntityOperationHandler(
    private val entityOperationService: EntityOperationService,
    private val authorizationService: AuthorizationService,
    private val entityEventService: EntityEventService
) {

    /**
     * Implements 6.14.3.1 - Create Batch of Entities
     */
    @PostMapping("/create", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun create(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        if (!authorizationService.userCanCreateEntities(sub))
            throw AccessDeniedException("User forbidden to create entities")

        val body = requestBody.awaitFirst()
        checkContext(httpHeaders, body)
        val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK))
        val (extractedEntities, _, ngsiLdEntities) =
            extractAndParseBatchOfEntities(body, context, httpHeaders.contentType)
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)
        val batchOperationResult = entityOperationService.create(newEntities)

        batchOperationResult.errors.addAll(
            existingEntities.map { entity ->
                BatchEntityError(entity.id, arrayListOf("Entity already exists"))
            }
        )

        authorizationService.createAdminLinks(batchOperationResult.getSuccessfulEntitiesIds(), sub)
        ngsiLdEntities
            .filter { it.id in batchOperationResult.getSuccessfulEntitiesIds() }
            .forEach {
                val entityPayload = extractEntityPayloadById(extractedEntities, it.id)
                entityEventService.publishEntityCreateEvent(
                    sub.orNull(),
                    it.id,
                    it.type,
                    extractContextFromInput(entityPayload)
                )
            }

        return if (batchOperationResult.errors.isEmpty())
            ResponseEntity.status(HttpStatus.CREATED).body(batchOperationResult.getSuccessfulEntitiesIds())
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(batchOperationResult)
    }

    private fun extractEntityPayloadById(entitiesPayload: List<Map<String, Any>>, entityId: URI): Map<String, Any> {
        return entitiesPayload.first {
            it["id"] == entityId.toString()
        }
    }

    /**
     * Implements 6.15.3.1 - Upsert Batch of Entities
     */
    @PostMapping("/upsert", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun upsert(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @RequestParam(required = false) options: String?
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val body = requestBody.awaitFirst()
        checkContext(httpHeaders, body)
        val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK))

        val (extractedEntities, jsonLdEntities, ngsiLdEntities) =
            extractAndParseBatchOfEntities(body, context, httpHeaders.contentType)
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)

        val createBatchOperationResult = when {
            newEntities.isEmpty() -> BatchOperationResult()
            authorizationService.userCanCreateEntities(sub) -> entityOperationService.create(newEntities)
            else -> BatchOperationResult(
                errors = ArrayList(
                    newEntities.map { BatchEntityError(it.id, arrayListOf("User forbidden to create entities")) }
                )
            )
        }

        authorizationService.createAdminLinks(createBatchOperationResult.getSuccessfulEntitiesIds(), sub)

        val existingEntitiesIdsAuthorized =
            authorizationService.filterEntitiesUserCanUpdate(
                existingEntities.map { it.id },
                sub
            )

        val (existingEntitiesAuthorized, existingEntitiesUnauthorized) =
            existingEntities.partition { existingEntitiesIdsAuthorized.contains(it.id) }

        val updateBatchOperationResult = when (options) {
            "update" -> entityOperationService.update(existingEntitiesAuthorized, false)
            else -> entityOperationService.replace(existingEntitiesAuthorized)
        }

        updateBatchOperationResult.errors.addAll(
            existingEntitiesUnauthorized.map {
                BatchEntityError(it.id, arrayListOf("User forbidden to modify entity"))
            }
        )

        val batchOperationResult = BatchOperationResult(
            ArrayList(createBatchOperationResult.success.plus(updateBatchOperationResult.success)),
            ArrayList(createBatchOperationResult.errors.plus(updateBatchOperationResult.errors))
        )

        ngsiLdEntities
            .filter { it.id in createBatchOperationResult.getSuccessfulEntitiesIds() }
            .forEach {
                val entityPayload = extractEntityPayloadById(extractedEntities, it.id)
                entityEventService.publishEntityCreateEvent(
                    sub.orNull(),
                    it.id,
                    it.type,
                    extractContextFromInput(entityPayload)
                )
            }
        if (options == "update") publishUpdateEvents(sub.orNull(), updateBatchOperationResult, jsonLdEntities)
        else publishReplaceEvents(sub.orNull(), updateBatchOperationResult, extractedEntities, ngsiLdEntities)

        return if (batchOperationResult.errors.isEmpty() && newEntities.isNotEmpty())
            ResponseEntity.status(HttpStatus.CREATED).body(newEntities.map { it.id })
        else if (batchOperationResult.errors.isEmpty() && newEntities.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(batchOperationResult)
    }

    /**
     * Implements 6.16.3.1 - Update Batch of Entities
     */
    @PostMapping("/update", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun update(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestBody requestBody: Mono<String>,
        @RequestParam options: Optional<String>
    ): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val body = requestBody.awaitFirst()
        checkContext(httpHeaders, body)
        val context = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK))
        val disallowOverwrite = options.map { it == QUERY_PARAM_OPTIONS_NOOVERWRITE_VALUE }.orElse(false)

        val (_, jsonLdEntities, ngsiLdEntities) =
            extractAndParseBatchOfEntities(body, context, httpHeaders.contentType)
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)

        val existingEntitiesIdsAuthorized =
            authorizationService.filterEntitiesUserCanUpdate(
                existingEntities.map { it.id },
                sub
            )
        val (existingEntitiesAuthorized, existingEntitiesUnauthorized) =
            existingEntities.partition { existingEntitiesIdsAuthorized.contains(it.id) }
        val updateBatchOperationResult = entityOperationService.update(existingEntitiesAuthorized, disallowOverwrite)
        updateBatchOperationResult.errors.addAll(
            existingEntitiesUnauthorized.map {
                BatchEntityError(it.id, arrayListOf("User forbidden to modify entity"))
            }
        )
        updateBatchOperationResult.errors.addAll(
            newEntities.map {
                BatchEntityError(it.id, arrayListOf("Entity does not exist"))
            }
        )
        val batchOperationResult = BatchOperationResult(
            ArrayList(updateBatchOperationResult.success),
            ArrayList(updateBatchOperationResult.errors)
        )
        publishUpdateEvents(sub.orNull(), updateBatchOperationResult, jsonLdEntities)

        return if (batchOperationResult.errors.isEmpty() && newEntities.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(batchOperationResult)
    }

    /**
     * Implements 6.17.3.1 - Delete Batch of Entities
     */
    @PostMapping("/delete", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun delete(@RequestBody requestBody: Mono<List<String>>): ResponseEntity<*> {
        val sub = getSubFromSecurityContext()
        val body = requestBody.awaitFirst()

        val (existingEntities, unknownEntities) = entityOperationService
            .splitEntitiesIdsByExistence(body.toListOfUri())

        val (entitiesUserCanAdmin, entitiesUserCannotAdmin) = authorizationService
            .splitEntitiesByUserCanAdmin(existingEntities, sub)

        val entitiesIdsToDelete = entitiesUserCanAdmin.toSet()
        val entitiesBeforeDelete = entitiesIdsToDelete.map {
            entityOperationService.getEntityCoreProperties(it)
        }
        val batchOperationResult = entityOperationService.delete(entitiesIdsToDelete)
        batchOperationResult.errors.addAll(
            unknownEntities.map { BatchEntityError(it, arrayListOf("Entity does not exist")) }
        )
        batchOperationResult.errors.addAll(
            entitiesUserCannotAdmin.map { BatchEntityError(it, arrayListOf("User forbidden to delete entity")) }
        )

        batchOperationResult.success.map { it.entityId }.forEach { uri ->
            val entity = entitiesBeforeDelete.find { it.id == uri }!!
            entityEventService.publishEntityDeleteEvent(sub.orNull(), entity.id, entity.type[0], entity.contexts)
        }

        return if (batchOperationResult.errors.isEmpty())
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        else
            ResponseEntity.status(HttpStatus.MULTI_STATUS).body(batchOperationResult)
    }

    private fun extractAndParseBatchOfEntities(
        payload: String,
        context: String?,
        contentType: MediaType?
    ): Triple<List<Map<String, Any>>, List<JsonLdEntity>, List<NgsiLdEntity>> {
        val rawEntities = JsonUtils.deserializeListOfObjects(payload)
        if (contentType == JSON_LD_MEDIA_TYPE && rawEntities.any { !it.containsKey(JSONLD_CONTEXT) })
            throw BadRequestDataException(
                "One or more entities do not contain an @context and the request Content-Type is application/ld+json"
            )

        val jsonldRawEntities =
            if (contentType == JSON_LD_MEDIA_TYPE) rawEntities
            else
                rawEntities.map { rawEntity ->
                    val jsonldRawEntity = rawEntity.toMutableMap()
                    jsonldRawEntity.putIfAbsent(JSONLD_CONTEXT, listOf(context))
                    jsonldRawEntity
                }

        return jsonldRawEntities.let {
            if (contentType == JSON_LD_MEDIA_TYPE)
                Pair(it, expandJsonLdEntities(it))
            else
                Pair(it, expandJsonLdEntities(it, listOf(context!!)))
        }
            .let { Triple(it.first, it.second, it.second.map { it.toNgsiLdEntity() }) }
    }

    private fun publishReplaceEvents(
        sub: String?,
        updateBatchOperationResult: BatchOperationResult,
        extractedEntities: List<Map<String, Any>>,
        ngsiLdEntities: List<NgsiLdEntity>
    ) = ngsiLdEntities.filter { it.id in updateBatchOperationResult.getSuccessfulEntitiesIds() }
        .forEach {
            val entityPayload = serializeObject(extractEntityPayloadById(extractedEntities, it.id))
            entityEventService.publishEntityReplaceEvent(
                sub,
                it.id,
                it.type,
                extractContextFromInput(entityPayload)
            )
        }

    private fun publishUpdateEvents(
        sub: String?,
        updateBatchOperationResult: BatchOperationResult,
        jsonLdEntities: List<JsonLdEntity>
    ) {
        updateBatchOperationResult.success.forEach {
            val jsonLdEntity = jsonLdEntities.find { jsonLdEntity -> jsonLdEntity.id.toUri() == it.entityId }!!
            entityEventService.publishAttributeAppendEvents(
                sub,
                it.entityId,
                jsonLdEntity.properties,
                it.updateResult!!,
                jsonLdEntity.contexts
            )
        }
    }
}
