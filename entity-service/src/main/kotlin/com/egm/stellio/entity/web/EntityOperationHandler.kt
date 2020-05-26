package com.egm.stellio.entity.web

import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.entity.util.ValidationUtils
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
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/ngsi-ld/v1/entityOperations")
class EntityOperationHandler(
    private val entityService: EntityService,
    private val validationUtils: ValidationUtils
) {

    private val logger = LoggerFactory.getLogger(javaClass)

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
                val existingEntities = validationUtils.getExistingEntities(it)
                val newEntities = validationUtils.getNewEntities(it)
                val validEntities = validationUtils.getValidEntities(newEntities)
                Triple(existingEntities, newEntities, validEntities)
            }
            .map {
                    entityService.processBatchOfEntities(it.first, it.second, it.third)
            }
            .map {
                ResponseEntity.status(HttpStatus.OK).body(it)
            }
    }
}
