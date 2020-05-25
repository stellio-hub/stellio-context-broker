package com.egm.stellio.entity.web

import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.egm.stellio.entity.util.extractAndParseBatchOfEntities
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.ok
import org.springframework.web.reactive.function.server.bodyToMono
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

    fun extractAndParseBatchOfEntities(payload: String): List<ExpandedEntity> {
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