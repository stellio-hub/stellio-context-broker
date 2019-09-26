package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.repository.Neo4jRepository
import com.egm.datahub.context.registry.service.JsonLDService
import com.egm.datahub.context.registry.service.NgsiLdParserService
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
    private val ngsiLdParserService: NgsiLdParserService,
    private val neo4JRepository: Neo4jRepository,
    private val jsonLDService: JsonLDService
) {

    private val logger = LoggerFactory.getLogger(EntityHandler::class.java)

    fun create(req: ServerRequest): Mono<ServerResponse> {

        return req.bodyToMono<String>()
            .map {
                ngsiLdParserService.parseEntity(it)
            }
            .map {
                neo4JRepository.createOrUpdateEntity(it.first, it.second)
            }.flatMap {
                created(URI("/ngsi-ld/v1/entities/$it")).build()
            }.onErrorResume {
                    when (it) {
                        is AlreadyExistingEntityException -> status(HttpStatus.CONFLICT).body(BodyInserters.fromObject(it.localizedMessage))
                        is EntityCreationException -> status(HttpStatus.INTERNAL_SERVER_ERROR).body(BodyInserters.fromObject(it.localizedMessage))
                        else -> badRequest().body(BodyInserters.fromObject(it.localizedMessage))
                    }
            }
    }

    fun getEntities(req: ServerRequest): Mono<ServerResponse> {
        val type = req.queryParam("type").orElse("")
        val q = req.queryParam("q").orElse("")

        if (q.isNullOrEmpty() && type.isNullOrEmpty()) {
            return badRequest().body(BodyInserters.fromObject("query or type have to be specified: generic query on entities NOT yet supported"))
        }
        return "".toMono()
                .map {
                    neo4JRepository.getEntities(q, type)
                }
                .flatMap {
                    ok().body(BodyInserters.fromObject(it))
                }
                .onErrorResume {
                    status(HttpStatus.INTERNAL_SERVER_ERROR).build()
                }
    }

    fun getByURI(req: ServerRequest): Mono<ServerResponse> {
        val uri = req.pathVariable("entityId")
        return uri.toMono()
                .map {
                    neo4JRepository.getNodesByURI(it)
                }
                .flatMap {
                    ok().body(BodyInserters.fromObject(it))
                }
                .onErrorResume {
                    status(HttpStatus.INTERNAL_SERVER_ERROR).build()
                }
    }

    fun expand(req: ServerRequest): Mono<ServerResponse> {
        return req.bodyToMono<String>()
            .map {
                jsonLDService.expandNgsiLDEntity(it)
            }.flatMap {
                ok().body(BodyInserters.fromObject(it))
            }
    }

    fun updateAttribute(req: ServerRequest): Mono<ServerResponse> {
        val attr = req.pathVariable("attrId")
        val uri = req.pathVariable("entityId")

        return req.bodyToMono<String>()
            .map {
                ngsiLdParserService.ngsiLdToUpdateQuery(it, uri, attr)
            }
            .map {
                neo4JRepository.updateEntity(it.first, it.second)
            }
            .flatMap {
                status(HttpStatus.NO_CONTENT).body(BodyInserters.fromObject(it))
            }
            .onErrorResume {
                when (it) {
                    is NotExistingEntityException -> status(HttpStatus.NOT_FOUND).body(BodyInserters.fromObject(it.localizedMessage))
                    else -> badRequest().body(BodyInserters.fromObject(it.localizedMessage))
                }
            }
    }
}
