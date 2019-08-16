package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.repository.Neo4jRepository
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.bodyToMono
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono


@Component
class Neo4jService(
        private val neo4JRepository: Neo4jRepository
) {

    fun create(req: ServerRequest): Mono<ServerResponse> {

        return req.bodyToMono<String>()
            .map {
                neo4JRepository.insertJsonLd(it.toString())
            }.flatMap {
               ok().body(BodyInserters.fromObject(it))
            }.onErrorResume {
               status(HttpStatus.INTERNAL_SERVER_ERROR).build()
            }
    }

    fun getEntitiesByLabel(req: ServerRequest): Mono<ServerResponse> {
        // TODO throw a 400 if no entityId provided
        val label = req.pathVariable("label")
        return label.toMono()
                .map {
                    neo4JRepository.getEntitiesByLabel(label)
                }
                .flatMap {
                     ok().body(BodyInserters.fromObject(it))
                }
                .onErrorResume {
                    status(HttpStatus.INTERNAL_SERVER_ERROR).build()
                }
    }
}
