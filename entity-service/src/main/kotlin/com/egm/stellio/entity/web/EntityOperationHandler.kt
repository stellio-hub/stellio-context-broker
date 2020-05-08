package com.egm.stellio.entity.web

import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.entity.util.ValidationUtils
import com.egm.stellio.entity.util.extractAndParseBatchOfEntities
import com.egm.stellio.shared.model.BadRequestDataException
import org.neo4j.ogm.config.ObjectMapperFactory.objectMapper
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.bodyToMono
import reactor.core.publisher.Mono
import java.lang.reflect.UndeclaredThrowableException

@Component
class EntityOperationHandler(
    private val entityService: EntityService,
    private val validationUtils: ValidationUtils
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun generatesProblemDetails(list: List<String>): String {
        return objectMapper().writeValueAsString(mapOf("ProblemDetails" to list))
    }

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
            .onErrorResume {
                when (it) {
                    is BadRequestDataException -> status(HttpStatus.BAD_REQUEST).body(BodyInserters.fromValue(it.message.toString()))
                    is UndeclaredThrowableException -> badRequest().body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.undeclaredThrowable.message.toString()))))
                    else -> badRequest().body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.message.toString()))))
                }
            }
    }
}
