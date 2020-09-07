package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.web.extractSubjectOrEmpty
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/ngsi-ld/v1/entityOperations")
class EntityOperationHandler(
    private val entityOperationService: EntityOperationService,
    private val authorizationService: AuthorizationService
) {

    /**
     * Implements 6.14.3.1 - Create Batch of Entities
     */
    @PostMapping("/create", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun create(@RequestBody body: Mono<String>): Mono<ResponseEntity<*>> {
        return extractSubjectOrEmpty().flatMap { userId ->
            if (!authorizationService.userCanCreateEntities(userId))
                throw AccessDeniedException("User forbidden to create entities")
            body.map {
                extractAndParseBatchOfEntities(it)
            }.map {
                val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(it)
                val batchOperationResult = entityOperationService.create(newEntities)

                batchOperationResult.errors.addAll(
                    existingEntities.map { entity ->
                        BatchEntityError(entity.id, arrayListOf("Entity already exists"))
                    }
                )

                authorizationService.createAdminLinks(batchOperationResult.success, userId)

                batchOperationResult
            }.map {
                ResponseEntity.status(HttpStatus.OK).body(it)
            }
        }
    }

    /**
     * Implements 6.15.3.1 - Upsert Batch of Entities
     */
    @PostMapping("/upsert", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun upsert(
        @RequestBody body: Mono<String>,
        @RequestParam(required = false) options: String?
    ): Mono<ResponseEntity<*>> {
        return body
            .map {
                extractAndParseBatchOfEntities(it)
            }
            .zipWith(extractSubjectOrEmpty())
            .map { expandedEntitiesAndUserId ->
                val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(
                    expandedEntitiesAndUserId.t1
                )

                val createBatchOperationResult =
                    if (authorizationService.userCanCreateEntities(expandedEntitiesAndUserId.t2))
                        entityOperationService.create(newEntities)
                    else
                        BatchOperationResult(
                            errors = ArrayList(
                                newEntities.map {
                                    BatchEntityError(it.id, arrayListOf("User forbidden to create entities"))
                                }
                            )
                        )

                authorizationService.createAdminLinks(createBatchOperationResult.success, expandedEntitiesAndUserId.t2)

                val existingEntitiesIdsAuthorized =
                    authorizationService.filterEntitiesUserCanUpdate(
                        existingEntities.map { it.id },
                        expandedEntitiesAndUserId.t2
                    )

                val (existingEntitiesAuthorized, existingEntitiesUnauthorized) =
                    existingEntities.partition { existingEntitiesIdsAuthorized.contains(it.id) }

                val updateBatchOperationResult = when (options) {
                    "update" -> entityOperationService.update(existingEntitiesAuthorized, createBatchOperationResult)
                    else -> entityOperationService.replace(existingEntitiesAuthorized, createBatchOperationResult)
                }

                updateBatchOperationResult.errors.addAll(
                    existingEntitiesUnauthorized.map {
                        BatchEntityError(it.id, arrayListOf("User forbidden to modify entity"))
                    }
                )

                BatchOperationResult(
                    ArrayList(createBatchOperationResult.success.plus(updateBatchOperationResult.success)),
                    ArrayList(createBatchOperationResult.errors.plus(updateBatchOperationResult.errors))
                )
            }
            .map {
                ResponseEntity.status(HttpStatus.OK).body(it)
            }
    }

    private fun extractAndParseBatchOfEntities(payload: String): List<NgsiLdEntity> {
        val extractedEntities = extractEntitiesFromJsonPayload(payload)
        return JsonLdUtils.expandJsonLdEntities(extractedEntities)
            .map {
                it.toNgsiLdEntity()
            }
    }

    private fun extractEntitiesFromJsonPayload(payload: String): List<Map<String, Any>> {
        val mapper = jacksonObjectMapper()
        return mapper.readValue(
            payload,
            mapper.typeFactory.constructCollectionType(MutableList::class.java, Map::class.java)
        )
    }
}
