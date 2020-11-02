package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.model.*
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
    private val entityHandler: EntityHandler,
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
        val (extractedEntities, _, ngsiLdEntities) = extractAndParseBatchOfEntities(body)

        val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(ngsiLdEntities)
        val batchOperationResult = entityOperationService.create(newEntities)

        batchOperationResult.errors.addAll(
            existingEntities.map { entity ->
                BatchEntityError(entity.id, arrayListOf("Entity already exists"))
            }
        )

        authorizationService.createAdminLinks(batchOperationResult.success.map { it.entityId }, userId)
        ngsiLdEntities.filter { it.id in batchOperationResult.success.map { it.entityId } }
            .forEach {
                entityEventService.publishEntityEvent(
                    EntityCreateEvent(it.id, serializeObject(extractEntityPayloadById(extractedEntities, it.id))),
                    it.type.extractShortTypeFromExpanded()
                )
            }

        return ResponseEntity.status(HttpStatus.OK).body(
            BatchOperationResponse(
                batchOperationResult.success.map { it.entityId }.toMutableList(),
                batchOperationResult.errors
            )
        )
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
        val (extractedEntities, jsonLdEntities, ngsiLdEntities) = extractAndParseBatchOfEntities(body)
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

        authorizationService.createAdminLinks(createBatchOperationResult.success.map { it.entityId }, userId)

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

        publishUpsertEvents(batchOperationResult, extractedEntities, jsonLdEntities, ngsiLdEntities)

        val batchOperationResponse = if (options == "update")
            buildBatchUpdateOperationResponse(createBatchOperationResult, updateBatchOperationResult)
        else
            BatchOperationResponse(
                batchOperationResult.success.map { it.entityId }.toMutableList(),
                batchOperationResult.errors
            )

        return ResponseEntity.status(HttpStatus.OK).body(batchOperationResponse)
    }

    @PostMapping("/delete", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun delete(@RequestBody requestBody: Mono<List<String>>): ResponseEntity<*> {
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

        return ResponseEntity.status(HttpStatus.OK).body(
            BatchOperationResponse(
                batchOperationResult.success.map { it.entityId }.toMutableList(),
                batchOperationResult.errors
            )
        )
    }

    private fun extractAndParseBatchOfEntities(payload: String):
        Triple<List<Map<String, Any>>, List<JsonLdEntity>, List<NgsiLdEntity>> =
            JsonUtils.parseListOfEntities(payload)
                .let { Pair(it, JsonLdUtils.expandJsonLdEntities(it)) }
                .let { Triple(it.first, it.second, it.second.map { it.toNgsiLdEntity() }) }

    private fun buildBatchUpdateOperationResponse(
        createBatchOperationResult: BatchOperationResult,
        updateBatchOperationResult: BatchOperationResult
    ) = updateBatchOperationResult.success
        .map { it as BatchEntityUpdateSuccess }
        .partition { it.updateAttributesResult.notUpdated.isEmpty() }
        .let {
            BatchOperationResult(
                it.first.toMutableList(),
                it.second.map {
                    BatchEntityError(
                        it.entityId,
                        ArrayList(
                            it.updateAttributesResult.notUpdated
                                .map { it.attributeName.extractShortTypeFromExpanded() + " : " + it.reason }
                        )
                    )
                }.toMutableList()
            )
        }
        .let {
            BatchOperationResult(
                ArrayList(createBatchOperationResult.success.plus(it.success)),
                ArrayList(createBatchOperationResult.errors.plus(it.errors))
            )
        }
        .let {
            BatchOperationResponse(
                it.success.map { it.entityId }.toMutableList(),
                it.errors
            )
        }

    private fun publishUpsertEvents(
        batchOperationResult: BatchOperationResult,
        extractedEntities: List<Map<String, Any>>,
        jsonLdEntities: List<JsonLdEntity>,
        ngsiLdEntities: List<NgsiLdEntity>
    ) {
        batchOperationResult.success.forEach {
            when (it) {
                is BatchEntityCreateSuccess -> entityEventService.publishEntityEvent(
                    EntityCreateEvent(
                        it.entityId,
                        serializeObject(
                            extractEntityPayloadById(extractedEntities, it.entityId)
                        )
                    ),
                    ngsiLdEntities.find { ngsiLdEntity -> ngsiLdEntity.id == it.entityId }!!
                        .let { it.type.extractShortTypeFromExpanded() }
                )
                is BatchEntityReplaceSuccess -> entityEventService.publishEntityEvent(
                    EntityReplaceEvent(
                        it.entityId,
                        serializeObject(
                            extractEntityPayloadById(extractedEntities, it.entityId)
                        )
                    ),
                    ngsiLdEntities.find { ngsiLdEntity -> ngsiLdEntity.id == it.entityId }!!
                        .let { it.type.extractShortTypeFromExpanded() }
                )
                is BatchEntityUpdateSuccess -> {
                    val jsonLdEntity = jsonLdEntities.find { jsonLdEntity -> jsonLdEntity.id.toUri() == it.entityId }!!
                    entityHandler.publishAppendEntityAttributesEvents(
                        it.entityId,
                        jsonLdEntity.properties,
                        it.updateAttributesResult,
                        jsonLdEntity.contexts
                    )
                }
            }
        }
    }
}
