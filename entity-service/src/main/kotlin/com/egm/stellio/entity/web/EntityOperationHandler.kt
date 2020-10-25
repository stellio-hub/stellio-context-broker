package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.web.extractSubjectOrEmpty
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.net.URI

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
    suspend fun create(@RequestBody requestBody: Mono<String>): ResponseEntity<*> {
        val userId = extractSubjectOrEmpty().awaitFirst()
        if (!authorizationService.userCanCreateEntities(userId))
            throw AccessDeniedException("User forbidden to create entities")

        val body = requestBody.awaitFirst()
        val ngsiLdEntities = extractAndParseBatchOfEntities(body)
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)
        val batchOperationResult = entityOperationService.create(newEntities)

        batchOperationResult.errors.addAll(
            existingEntities.map { entity ->
                BatchEntityError(entity.id, arrayListOf("Entity already exists"))
            }
        )

        val extractedEntities = JsonUtils.parseListOfEntities(body)
        authorizationService.createAdminLinks(batchOperationResult.success, userId)
        ngsiLdEntities.filter { it.id in batchOperationResult.success }
            .map { Pair(it, extractEntityPayloadById(extractedEntities, it.id)) }
            .map {
                entityEventService.publishEntityEvent(
                    EntityCreateEvent(it.first.id, serializeObject(it.second)),
                    it.first.type.extractShortTypeFromExpanded()
                )
            }

        return ResponseEntity.status(HttpStatus.OK).body(batchOperationResult)
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
        @RequestBody requestBody: Mono<String>,
        @RequestParam(required = false) options: String?
    ): ResponseEntity<*> {
        val userId = extractSubjectOrEmpty().awaitFirst()
        val body = requestBody.awaitFirst()
        val ngsiLdEntities = extractAndParseBatchOfEntities(body)
        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)

        val createBatchOperationResult = when {
            newEntities.isEmpty() -> BatchOperationResult()
            authorizationService.userCanCreateEntities(userId) -> entityOperationService.create(newEntities)
            else -> BatchOperationResult(
                errors = ArrayList(
                    newEntities.map {
                        BatchEntityError(it.id, arrayListOf("User forbidden to create entities"))
                    }
                )
            )
        }

        authorizationService.createAdminLinks(createBatchOperationResult.success, userId)

        val existingEntitiesIdsAuthorized =
            authorizationService.filterEntitiesUserCanUpdate(
                existingEntities.map { it.id },
                userId
            )

        val (existingEntitiesAuthorized, existingEntitiesUnauthorized) =
            existingEntities.partition { existingEntitiesIdsAuthorized.contains(it.id) }

        val updateBatchOperationResult = when (options) {
            "update" -> entityOperationService.update(existingEntitiesAuthorized)
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

        return ResponseEntity.status(HttpStatus.OK).body(batchOperationResult)
    }

    @PostMapping("/delete", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun delete(@RequestBody requestBody: Mono<List<String>>): ResponseEntity<BatchOperationResult> {
        val userId = extractSubjectOrEmpty().awaitFirst()
        val body = requestBody.awaitFirst()

        val (existingEntities, unknownEntities) = entityOperationService
            .splitEntitiesIdsByExistence(body.toListOfUri())

        val (entitiesUserCanAdmin, entitiesUserCannotAdmin) = authorizationService
            .splitEntitiesByUserCanAdmin(existingEntities, userId)

        val batchOperationResult = entityOperationService.delete(entitiesUserCanAdmin.toSet())
        batchOperationResult.errors.addAll(
            unknownEntities.map { BatchEntityError(it, arrayListOf("Entity does not exist")) }
        )
        batchOperationResult.errors.addAll(
            entitiesUserCannotAdmin.map { BatchEntityError(it, arrayListOf("User forbidden to delete entity")) }
        )

        return ResponseEntity.status(HttpStatus.OK).body(batchOperationResult)
    }

    private fun extractAndParseBatchOfEntities(payload: String): List<NgsiLdEntity> {
        val extractedEntities = JsonUtils.parseListOfEntities(payload)
        return JsonLdUtils.expandJsonLdEntities(extractedEntities)
            .map {
                it.toNgsiLdEntity()
            }
    }
}
