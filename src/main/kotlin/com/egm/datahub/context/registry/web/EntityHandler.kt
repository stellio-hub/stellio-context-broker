package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.repository.GraphDBRepository
import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.Rio
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.bodyToMono
import reactor.core.publisher.Mono
import java.net.URI


@Component
class EntityHandler(
        private val graphDBRepository: GraphDBRepository
) {

    fun create(req: ServerRequest): Mono<ServerResponse> {
        var originJsonLD = ""
        var entityUrn = ""
        return req.bodyToMono<String>()
            .log()
            .map {
                originJsonLD = it
                Rio.parse(it.reader(), "", RDFFormat.JSONLD)
            }
            .map {
                entityUrn = it.toList().find { it.subject.stringValue().startsWith("urn:") }?.subject.toString()
                graphDBRepository.createEntity(originJsonLD, it)
            }.flatMap {
                created(URI("/ngsi-ld/v1/entities/$entityUrn")).build()
            }.onErrorResume {
                ServerResponse.badRequest().body(BodyInserters.fromObject(it.localizedMessage))
            }
    }

    fun getById(req: ServerRequest): Mono<ServerResponse> {
        // TODO throw a 400 if no entityId provided
        val entityId = req.pathVariable("entityId")

        val result = graphDBRepository.getById(entityId)

        return ok().body(BodyInserters.fromObject(result))
    }
}
