package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.model.EntityEvent
import com.egm.datahub.context.registry.model.EventType
import com.egm.datahub.context.registry.repository.Neo4jRepository
import com.egm.datahub.context.registry.service.Neo4jService
import com.egm.datahub.context.registry.service.NgsiLdParserService
import org.neo4j.ogm.response.model.NodeModel
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
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
    private val neo4jService: Neo4jService,
    private val applicationEventPublisher: ApplicationEventPublisher
) {

    private val logger = LoggerFactory.getLogger(EntityHandler::class.java)

    fun create(req: ServerRequest): Mono<ServerResponse> {
        return req.bodyToMono<String>()
            .map {
                val urn = ngsiLdParserService.extractEntityUrn(it)
                if (neo4JRepository.checkExistingUrn(urn)) {
                    throw AlreadyExistingEntityException("$urn already existing")
                }
                ngsiLdParserService.parseEntity(it)
            }
            .map {
                neo4JRepository.createEntity(it.entityUrn, it.entityStatements, it.relationshipStatements)
                it
            }.map {
                val entityEvent = EntityEvent(it.entityType, it.entityUrn, EventType.POST, it.ngsiLdPayload)
                applicationEventPublisher.publishEvent(entityEvent)
                it.entityUrn
            }
            .flatMap {
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
                .map {
                    it.map {
                        neo4jService.queryResultToNgsiLd(it.get("n") as NodeModel)
                    }
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
                    neo4JRepository.getNodeByURI(it)
                }
                .map {
                    neo4jService.queryResultToNgsiLd(it.get("n") as NodeModel)
                }
                .flatMap {
                    ok().body(BodyInserters.fromObject(it))
                }
                .onErrorResume {
                    status(HttpStatus.INTERNAL_SERVER_ERROR).build()
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
                neo4JRepository.updateEntity(it, uri)
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
