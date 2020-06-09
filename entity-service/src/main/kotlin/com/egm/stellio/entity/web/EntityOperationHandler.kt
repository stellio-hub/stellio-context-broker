package com.egm.stellio.entity.web

import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils
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
    private val entityOperationService: EntityOperationService
) {

    /**
     * Implements 6.14.3.1 - Create Batch of Entities
     */
    @PostMapping("/create", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun create(@RequestBody body: Mono<String>): Mono<ResponseEntity<*>> {

        return body
            .map {
                extractAndParseBatchOfEntities(it)
            }
            .map {
                val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(it)
                val batchOperationResult = entityOperationService.create(newEntities)

                batchOperationResult.errors.addAll(
                    existingEntities.map { entity ->
                        BatchEntityError(entity.id, arrayListOf("Entity already exists"))
                    })

                batchOperationResult
            }
            .map {
                ResponseEntity.status(HttpStatus.OK).body(it)
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
            .map {
                val (existingEntities, newEntities) = entityOperationService.splitEntitiesByExistence(it)

                val createBatchOperationResult = entityOperationService.create(newEntities)

                val updateBatchOperationResult = when (options) {
                    "update" -> entityOperationService.update(existingEntities, createBatchOperationResult)
                    else -> entityOperationService.replace(existingEntities, createBatchOperationResult)
                }

                BatchOperationResult(
                    ArrayList(createBatchOperationResult.success.plus(updateBatchOperationResult.success)),
                    ArrayList(createBatchOperationResult.errors.plus(updateBatchOperationResult.errors))
                )
            }
            .map {
                ResponseEntity.status(HttpStatus.OK).body(it)
            }
    }

    private fun extractAndParseBatchOfEntities(payload: String): List<ExpandedEntity> {
        val extractedEntities = extractEntitiesFromJsonPayload(payload)
        return NgsiLdParsingUtils.parseEntities(extractedEntities)
    }

    private fun extractEntitiesFromJsonPayload(payload: String): List<Map<String, Any>> {
        val mapper = jacksonObjectMapper()
        return mapper.readValue(
            payload,
            mapper.typeFactory.constructCollectionType(MutableList::class.java, Map::class.java)
        )
    }
}
