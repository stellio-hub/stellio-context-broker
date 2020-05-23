package com.egm.stellio.entity.web

import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.entity.util.ValidationUtils
import com.egm.stellio.entity.util.extractAndParseBatchOfEntities
import com.egm.stellio.shared.util.ApiUtils.serializeObject
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/ngsi-ld/v1/entityOperations")class EntityOperationHandler(
    private val entityService: EntityService,
    private val validationUtils: ValidationUtils
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Implements 6.14.3.1 - Create Batch of Entities
     */
    @PostMapping("/create")
    fun create(@RequestBody entities: Mono<String>): Mono<ResponseEntity<String>> {

        return entities
            .map {
                extractAndParseBatchOfEntities(it)
            }
            .map {
                val existingEntities = validationUtils.getExistingEntities(it)
                val newEntities = validationUtils.getNewEntities(it)
                val validEntities = validationUtils.getValidEntities(newEntities)
                Triple(existingEntities, newEntities, validEntities)
            }
            .map {
                    entityService.processBatchOfEntities(it.first, it.second, it.third)
            }
            .map {
                ResponseEntity.ok().body(serializeObject(it))
            }
    }
}
