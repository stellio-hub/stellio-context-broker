package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.service.Neo4jService
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.getTypeFromURI
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
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
import reactor.kotlin.core.publisher.toMono
import java.lang.reflect.UndeclaredThrowableException
import java.net.URI

@Component
class EntityHandler(
    private val neo4jService: Neo4jService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun generatesProblemDetails(list: List<String>): String {
        return objectMapper().writeValueAsString(mapOf("ProblemDetails" to list))
    }

    /**
     * Implements 6.4.3.1 - Create Entity
     */
    fun create(req: ServerRequest): Mono<ServerResponse> {

        return req.bodyToMono<String>()
            .map {
                NgsiLdParsingUtils.parseEntity(it)
            }
            .map {
                // TODO validation (https://redmine.eglobalmark.com/issues/853)
                val urn = it.first.getOrElse("@id") { "" } as String
                if (neo4jService.exists(urn)) {
                    throw AlreadyExistsException("Already Exists")
                }

                it
            }
            .map {
                neo4jService.createEntity(it.first, it.second)
            }
            .flatMap {
                created(URI("/ngsi-ld/v1/entities/${it.id}")).build()
            }.onErrorResume {
                when (it) {
                    is AlreadyExistsException -> status(HttpStatus.CONFLICT).build()
                    is InternalErrorException -> status(HttpStatus.INTERNAL_SERVER_ERROR).build()
                    is BadRequestDataException -> status(HttpStatus.BAD_REQUEST).body(BodyInserters.fromValue(it.message.toString()))
                    is UndeclaredThrowableException -> badRequest().body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.undeclaredThrowable.message.toString()))))
                    else -> badRequest().body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.message.toString()))))
                }
            }
    }

    /**
     * Implements 6.4.3.2 - Query Entities
     */
    fun getEntities(req: ServerRequest): Mono<ServerResponse> {
        val type = req.queryParam("type").orElse("")
        val q = req.queryParams()["q"].orEmpty()

        val contextLink = extractContextFromLinkHeader(req)

        // TODO 6.4.3.2 says that either type or attrs must be provided (and not type or q)
        if (q.isNullOrEmpty() && type.isNullOrEmpty()) {
            return badRequest().body(BodyInserters.fromValue("'q' or 'type' request parameters have to be specified (TEMP - cf 6.4.3.2"))
        }

        if (!NgsiLdParsingUtils.isTypeResolvable(type, contextLink)) {
            return badRequest().body(BodyInserters.fromValue("Unable to resolve 'type' parameter from the provided Link header"))
        }

        return "".toMono()
            .map {
                neo4jService.searchEntities(type, q, contextLink)
            }
            .map {
                it.map {
                    JsonLdProcessor.compact(it.first, mapOf("@context" to it.second), JsonLdOptions())
                }
            }
            .flatMap {
                ok().body(BodyInserters.fromValue(it))
            }
            .onErrorResume {
                badRequest().body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.message.toString()))))
            }
    }

    /**
     * Implements 6.5.3.1 - Retrieve Entity
     */
    fun getByURI(req: ServerRequest): Mono<ServerResponse> {
        val uri = req.pathVariable("entityId")
        return uri.toMono()
            .map {
                if (!neo4jService.exists(uri)) throw ResourceNotFoundException("Entity Not Found")
                neo4jService.getFullEntityById(it)
            }
            .map {
                JsonLdProcessor.compact(it.first, mapOf("@context" to it.second), JsonLdOptions())
            }
            .flatMap {
                ok().body(BodyInserters.fromValue(it))
            }
            .onErrorResume {
                when (it) {
                    is ResourceNotFoundException -> status(HttpStatus.NOT_FOUND).build()
                    else -> badRequest().body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.message.toString()))))
                }
            }
    }

    /**
     * Implements 6.6.3.2 - Update Entity Attributes
     *
     * Current implementation is basic and only update values of properties.
     */
    fun updateEntityAttributes(req: ServerRequest): Mono<ServerResponse> {
        val uri = req.pathVariable("entityId")
        val type = getTypeFromURI(uri)
        val contextLink = extractContextFromLinkHeader(req)

        return req.bodyToMono<String>()
            .map {
                if (!NgsiLdParsingUtils.isTypeResolvable(type, contextLink)) {
                    throw BadRequestDataException("Unable to resolve 'type' parameter from the provided Link header")
                }
                neo4jService.updateEntityAttributes(uri, it, contextLink)
            }
            .flatMap {
                logger.debug("Updated ${it.size} attributes on entity $uri")
                status(HttpStatus.NO_CONTENT).build()
            }
            .onErrorResume {
                when (it) {
                    is ResourceNotFoundException -> status(HttpStatus.NOT_FOUND).body(BodyInserters.fromValue(it.localizedMessage))
                    else -> badRequest().body(BodyInserters.fromValue(it.localizedMessage))
                }
            }
    }

    /**
     * Implements 6.7.3.1 - Partial Attribute Update
     */
    fun partialAttributeUpdate(req: ServerRequest): Mono<ServerResponse> {
        val attr = req.pathVariable("attrId")
        val uri = req.pathVariable("entityId")
        val type = getTypeFromURI(uri)

        val contextLink = extractContextFromLinkHeader(req)

        return req.bodyToMono<String>()
            .map {
                if (!NgsiLdParsingUtils.isTypeResolvable(type, contextLink)) {
                    throw BadRequestDataException("Unable to resolve 'type' parameter from the provided Link header")
                }
                neo4jService.updateEntityAttribute(uri, attr, it, contextLink)
            }
            .flatMap {
                status(HttpStatus.NO_CONTENT).build()
            }
            .onErrorResume {
                when (it) {
                    is ResourceNotFoundException -> status(HttpStatus.NOT_FOUND).body(BodyInserters.fromValue(it.localizedMessage))
                    else -> badRequest().body(BodyInserters.fromValue(it.localizedMessage))
                }
            }
    }

    /**
     * Implements 6.5.3.2 - Delete Entity
     */
    fun delete(req: ServerRequest): Mono<ServerResponse> {
        val entityId = req.pathVariable("entityId")

        return entityId.toMono()
            .map {
                neo4jService.deleteEntity(entityId)
            }
            .flatMap {
                if (it.first >= 1)
                    noContent().build()
                else
                    notFound().build()
            }
            .onErrorResume {
                status(HttpStatus.INTERNAL_SERVER_ERROR).body(BodyInserters.fromValue(generatesProblemDetails(listOf(it.localizedMessage))))
            }
    }

    /**
     * As per 6.3.5, extract @context from Link header. In the absence of such Link header, it returns the default
     * JSON-LD @context.
     */
    fun extractContextFromLinkHeader(req: ServerRequest): String {
        return if (req.headers().header("Link").isNotEmpty() && req.headers().header("Link").get(0) != null)
            req.headers().header("Link")[0].split(";")[0].removePrefix("<").removeSuffix(">")
        else
            NgsiLdParsingUtils.NGSILD_CORE_CONTEXT
    }
}
