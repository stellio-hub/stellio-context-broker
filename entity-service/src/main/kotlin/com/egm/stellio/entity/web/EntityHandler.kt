package com.egm.stellio.entity.web

import com.egm.stellio.entity.service.Neo4jService
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.shared.util.NgsiLdParsingUtils.getTypeFromURI
import com.egm.stellio.entity.util.decode
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.extractContextFromLinkHeader
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
                NgsiLdParsingUtils.parseEntity(it, NgsiLdParsingUtils.getContextOrThrowError(it))
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

        /* Decoding query parameters is not supported by default so a call to a decode function was added query with the right parameters values */
        return "".toMono()
            .map {
                neo4jService.searchEntities(type, q.decode(), contextLink)
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
     * Implements 6.6.3.1 - Append Entity Attributes
     *
     */
    fun appendEntityAttributes(req: ServerRequest): Mono<ServerResponse> {
        val entityId = req.pathVariable("entityId")
        val disallowOverwrite = req.queryParam("options").map { it == "noOverwrite" }.orElse(false)
        val type = getTypeFromURI(entityId)
        val contextLink = extractContextFromLinkHeader(req)
        return req.bodyToMono<String>()
            .doOnNext {
                if (!NgsiLdParsingUtils.isTypeResolvable(type, contextLink)) {
                    throw BadRequestDataException("Unable to resolve 'type' parameter from the provided Link header")
                }

                if (!neo4jService.exists(entityId)) throw ResourceNotFoundException("Entity $entityId does not exist")
            }
            .map {
                NgsiLdParsingUtils.expandJsonLdFragment(it, contextLink)
            }
            .map {
                neo4jService.appendEntityAttributes(entityId, it, disallowOverwrite)
            }
            .flatMap {
                logger.debug("Appended $it attributes on entity $entityId")
                if (it.notUpdated.isEmpty())
                    status(HttpStatus.NO_CONTENT).build()
                else
                    status(HttpStatus.MULTI_STATUS).body(BodyInserters.fromValue(it))
            }
            .onErrorResume {
                when (it) {
                    is ResourceNotFoundException -> status(HttpStatus.NOT_FOUND).body(BodyInserters.fromValue(it.localizedMessage))
                    else -> badRequest().body(BodyInserters.fromValue(it.localizedMessage))
                }
            }
    }

    /**
     * Implements 6.6.3.2 - Update Entity Attributes
     *
     */
    fun updateEntityAttributes(req: ServerRequest): Mono<ServerResponse> {
        val uri = req.pathVariable("entityId")
        val type = getTypeFromURI(uri)
        val contextLink = extractContextFromLinkHeader(req)

        return req.bodyToMono<String>()
            .doOnNext {
                if (!neo4jService.exists(uri)) throw ResourceNotFoundException("Entity $uri does not exist")

                if (!NgsiLdParsingUtils.isTypeResolvable(type, contextLink)) {
                    throw BadRequestDataException("Unable to resolve 'type' parameter from the provided Link header")
                }
            }
            .map {
                neo4jService.updateEntityAttributes(uri, it, contextLink)
            }
            .flatMap {
                logger.debug("Update $it attributes on entity $uri")
                if (it.notUpdated.isEmpty())
                    status(HttpStatus.NO_CONTENT).build()
                else
                    status(HttpStatus.MULTI_STATUS).body(BodyInserters.fromValue(it))
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
     * Current implementation is basic and only update the value of a property.
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
}
