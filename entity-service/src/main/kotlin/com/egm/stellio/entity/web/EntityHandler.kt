package com.egm.stellio.entity.web

import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.entity.util.decode
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.extractContextFromLinkHeader
import com.egm.stellio.shared.util.ApiUtils.serializeObject
import com.egm.stellio.shared.util.NgsiLdParsingUtils.compactEntities
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.generatesProblemDetails
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.bodyToMono
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.net.URI

@Component
class EntityHandler(
    private val entityService: EntityService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

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
                if (entityService.exists(it.id)) {
                    throw AlreadyExistsException("Already Exists")
                }

                it
            }
            .map {
                entityService.createEntity(it)
            }
            .flatMap {
                created(URI("/ngsi-ld/v1/entities/${it.id}")).build()
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
        if (q.isNullOrEmpty() && type.isNullOrEmpty())
            return badRequest().contentType(MediaType.APPLICATION_JSON).bodyValue(
                generatesProblemDetails(ErrorResponse(ErrorType.BAD_REQUEST_DATA, "The request includes input data which does not meet the requirements of the operation",
                        "'q' or 'type' request parameters have to be specified (TEMP - cf 6.4.3.2")))

        if (!NgsiLdParsingUtils.isTypeResolvable(type, contextLink))
            return badRequest().contentType(MediaType.APPLICATION_JSON).bodyValue(
                generatesProblemDetails(ErrorResponse(ErrorType.BAD_REQUEST_DATA, "The request includes input data which does not meet the requirements of the operation",
                    "Unable to resolve 'type' parameter from the provided Link header")))

        /* Decoding query parameters is not supported by default so a call to a decode function was added query with the right parameters values */
        return "".toMono()
            .map {
                entityService.searchEntities(type, q.decode(), contextLink)
            }
            .map {
                compactEntities(it)
            }
            .flatMap {
                ok().body(BodyInserters.fromValue(serializeObject(it)))
            }
    }

    /**
     * Implements 6.5.3.1 - Retrieve Entity
     */
    fun getByURI(req: ServerRequest): Mono<ServerResponse> {
        val uri = req.pathVariable("entityId")
        return uri.toMono()
            .map {
                if (!entityService.exists(uri)) throw ResourceNotFoundException("Entity Not Found")
                entityService.getFullEntityById(it)
            }
            .map {
                it.compact()
            }
            .flatMap {
                ok().body(BodyInserters.fromValue(serializeObject(it)))
            }
    }

    /**
     * Implements 6.6.3.1 - Append Entity Attributes
     *
     */
    fun appendEntityAttributes(req: ServerRequest): Mono<ServerResponse> {
        val entityId = req.pathVariable("entityId")
        val disallowOverwrite = req.queryParam("options").map { it == "noOverwrite" }.orElse(false)
        val contextLink = extractContextFromLinkHeader(req)
        return req.bodyToMono<String>()
            .doOnNext {
                if (!entityService.exists(entityId)) throw ResourceNotFoundException("Entity $entityId does not exist")
            }
            .map {
                NgsiLdParsingUtils.expandJsonLdFragment(it, contextLink)
            }
            .map {
                entityService.appendEntityAttributes(entityId, it, disallowOverwrite)
            }
            .flatMap {
                logger.debug("Appended $it attributes on entity $entityId")
                if (it.notUpdated.isEmpty())
                    status(HttpStatus.NO_CONTENT).build()
                else
                    status(HttpStatus.MULTI_STATUS).body(BodyInserters.fromValue(it))
            }
    }

    /**
     * Implements 6.6.3.2 - Update Entity Attributes
     *
     */
    fun updateEntityAttributes(req: ServerRequest): Mono<ServerResponse> {
        val uri = req.pathVariable("entityId")
        val contextLink = extractContextFromLinkHeader(req)

        return req.bodyToMono<String>()
            .doOnNext {
                if (!entityService.exists(uri)) throw ResourceNotFoundException("Entity $uri does not exist")
            }
            .map {
                entityService.updateEntityAttributes(uri, it, contextLink)
            }
            .flatMap {
                logger.debug("Update $it attributes on entity $uri")
                if (it.notUpdated.isEmpty())
                    status(HttpStatus.NO_CONTENT).build()
                else
                    status(HttpStatus.MULTI_STATUS).body(BodyInserters.fromValue(it))
            }
    }

    /**
     * Implements 6.7.3.1 - Partial Attribute Update
     * Current implementation is basic and only update the value of a property.
     */
    fun partialAttributeUpdate(req: ServerRequest): Mono<ServerResponse> {
        val attr = req.pathVariable("attrId")
        val uri = req.pathVariable("entityId")
        val contextLink = extractContextFromLinkHeader(req)

        return req.bodyToMono<String>()
            .map {
                entityService.updateEntityAttribute(uri, attr, it, contextLink)
            }
            .flatMap {
                status(HttpStatus.NO_CONTENT).build()
            }
    }

    /**
     * Implements 6.5.3.2 - Delete Entity
     */
    fun delete(req: ServerRequest): Mono<ServerResponse> {
        val entityId = req.pathVariable("entityId")

        return entityId.toMono()
            .map {
                entityService.deleteEntity(entityId)
            }
            .flatMap {
                if (it.first >= 1)
                    noContent().build()
                else
                    notFound().build()
            }
    }
}
