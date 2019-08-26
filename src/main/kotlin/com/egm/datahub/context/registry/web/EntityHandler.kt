package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.repository.Neo4jRepository
import com.egm.datahub.context.registry.service.JsonLDService
import io.netty.util.internal.StringUtil.isNullOrEmpty
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
                    when (it) {
                        is AlreadyExistingEntityException -> ServerResponse.status(HttpStatus.CONFLICT).body(BodyInserters.fromObject(it.localizedMessage))
                        else -> ServerResponse.badRequest().body(BodyInserters.fromObject(it.localizedMessage))
                    }
            }
    }

    fun getEntities(req: ServerRequest): Mono<ServerResponse> {
        val type = req.queryParam("type").orElse("")
        val q = req.queryParam("q").orElse("")

        if (isNullOrEmpty(q) && isNullOrEmpty(type)){
            return ServerResponse.badRequest().body(BodyInserters.fromObject("query or type have to be specified: generic query on entities NOT yet supported"))
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
                    neo4JRepository.getByURI(it)
                }
                .flatMap {
                    ok().body(BodyInserters.fromObject(it))
                }
                .onErrorResume {
                    status(HttpStatus.INTERNAL_SERVER_ERROR).build()
                }
    }

}
