package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.repository.Neo4jRepository
import com.egm.datahub.context.registry.service.JsonLDService
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.bodyToMono
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import java.net.URI


@Component
class EntityHandler(
    private val jsonLDService: JsonLDService,
    private val neo4JRepository: Neo4jRepository
) {

    private val logger = LoggerFactory.getLogger(EntityHandler::class.java)

    fun create(req: ServerRequest): Mono<ServerResponse> {
        var entityUrn = ""

        return req.bodyToMono<String>()
            .map {
                entityUrn = jsonLDService.parsePayload(it)
                it
            }
            .doOnError {
                logger.error("JSON-LD parsing raised an error : ${it.message}")
            }
            .map {
                neo4JRepository.createEntity(it)
            }.flatMap {
                created(URI("/ngsi-ld/v1/entities/$entityUrn")).build()
            }.onErrorResume {
                ServerResponse.badRequest().body(BodyInserters.fromObject(it.localizedMessage))
            }
    }

    fun getByType(req: ServerRequest): Mono<ServerResponse> {
        val type = req.queryParam("type").orElse("") as String
        return type.toMono()
            .map {
                neo4JRepository.getEntitiesByLabel(it)
            }
            .flatMap {
                ok().body(BodyInserters.fromObject(it))
            }
            .onErrorResume {
                status(HttpStatus.INTERNAL_SERVER_ERROR).build()
            }
    }
}
