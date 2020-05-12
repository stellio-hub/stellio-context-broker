package com.egm.stellio.entity.web

import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.entity.util.ValidationUtils
import com.egm.stellio.entity.util.extractAndParseBatchOfEntities
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.bodyToMono
import reactor.core.publisher.Mono

@Component
class EntityOperationHandler(
    private val entityService: EntityService,
    private val validationUtils: ValidationUtils
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Implements 6.14.3.1 - Create Batch of Entities
     */
    fun create(req: ServerRequest): Mono<ServerResponse> {

        return req.bodyToMono<String>()
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
            .flatMap {
                ok().body(BodyInserters.fromValue(it))
            }
    }
}
