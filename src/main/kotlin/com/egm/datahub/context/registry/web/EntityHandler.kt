package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.repository.Neo4jRepository
import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.Rio
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
    private val neo4JRepository: Neo4jRepository
) {

    fun create(req: ServerRequest): Mono<ServerResponse> {
        var entityUrn = ""

        return req.bodyToMono<String>()
            .map {
                val model = Rio.parse(it.reader(), "", RDFFormat.JSONLD)
                entityUrn = model.toList().find { it.subject.stringValue().startsWith("urn:") }?.subject.toString()
                it
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
