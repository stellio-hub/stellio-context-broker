package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.repository.GraphDBRepository
import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.Rio
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.bodyToMono
import reactor.core.publisher.Mono

@Component
class EntityHandler(
        private val graphDBRepository: GraphDBRepository
) {

    private val logger = LoggerFactory.getLogger(EntityHandler::class.java)

    fun create(req: ServerRequest): Mono<ServerResponse> {
        return req.bodyToMono<String>()
            .map {
                Rio.parse(it.reader(), "", RDFFormat.JSONLD)
            }
            .log()
            .map {
                graphDBRepository.createEntity(it.toList())
            }.flatMap {
                it.fold(
                        { status(HttpStatus.INTERNAL_SERVER_ERROR).build() },
                        { ok().build() }
                )
            }
    }

    fun retrieve(req: ServerRequest): Mono<ServerResponse> {
        val type = req.queryParam("type")

        return type
            .map {
                graphDBRepository.getByType(it)
                    .fold(
                            { status(HttpStatus.INTERNAL_SERVER_ERROR).build() },
                            { ok().body(BodyInserters.fromObject(it)) }
                    )
            }
            .orElse(badRequest().build())
    }
}
