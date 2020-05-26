package com.egm.stellio.entity.web

import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.entity.util.decode
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.extractContextFromLinkHeader
import com.egm.stellio.shared.util.NgsiLdParsingUtils.compactEntities
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.ApiUtils.serializeObject
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_MERGE_PATCH_CONTENT_TYPE
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.net.URI
import java.util.*

@RestController
@RequestMapping("/ngsi-ld/v1/entities")
class EntityHandler(
    private val entityService: EntityService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Implements 6.4.3.1 - Create Entity
     */
    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun create(@RequestBody body: Mono<String>): Mono<ResponseEntity<*>> {
        return body
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
            .map {
                ResponseEntity.status(HttpStatus.CREATED).location(URI("/ngsi-ld/v1/entities/${it.id}")).build<String>()
            }
    }

    /**
     * Implements 6.4.3.2 - Query Entities
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun getEntities(@RequestHeader httpHeaders: HttpHeaders, @RequestParam params: MultiValueMap<String, String>):
            Mono<ResponseEntity<*>> {
        val type = params.getFirst("type") ?: ""
        val q = params.getOrDefault("q", emptyList())

        val contextLink = extractContextFromLinkHeader(httpHeaders.getOrEmpty("Link"))

        // TODO 6.4.3.2 says that either type or attrs must be provided (and not type or q)
        if (q.isNullOrEmpty() && type.isEmpty())
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(BadRequestDataResponse("'q' or 'type' request parameters have to be specified (TEMP - cf 6.4.3.2"))
                .toMono()

        if (!NgsiLdParsingUtils.isTypeResolvable(type, contextLink))
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(BadRequestDataResponse("Unable to resolve 'type' parameter from the provided Link header"))
                .toMono()

        /* Decoding query parameters is not supported by default so a call to a decode function was added query with the right parameters values */
        return "".toMono()
            .map {
                entityService.searchEntities(type, q.decode(), contextLink)
            }
            .map {
                compactEntities(it)
            }
            .map {
                ResponseEntity.status(HttpStatus.OK).body(serializeObject(it))
            }
    }

    /**
     * Implements 6.5.3.1 - Retrieve Entity
     */
    @GetMapping("/{entityId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun getByURI(@PathVariable entityId: String): Mono<ResponseEntity<*>> {
        return entityId.toMono()
            .map {
                if (!entityService.exists(entityId)) throw ResourceNotFoundException("Entity Not Found")
                entityService.getFullEntityById(it)
            }
            .map {
                it.compact()
            }
            .map {
                ResponseEntity.status(HttpStatus.OK).body(serializeObject(it))
            }
    }

    /**
     * Implements 6.6.3.1 - Append Entity Attributes
     *
     */
    @PostMapping("/{entityId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun appendEntityAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @RequestParam options: Optional<String>,
        @RequestBody body: Mono<String>
    ): Mono<ResponseEntity<*>> {
        val disallowOverwrite = options.map { it == "noOverwrite" }.orElse(false)
        val contextLink = extractContextFromLinkHeader(httpHeaders.getOrEmpty("Link"))
        return body
            .doOnNext {
                if (!entityService.exists(entityId)) throw ResourceNotFoundException("Entity $entityId does not exist")
            }
            .map {
                NgsiLdParsingUtils.expandJsonLdFragment(it, contextLink)
            }
            .map {
                entityService.appendEntityAttributes(entityId, it, disallowOverwrite)
            }
            .map {
                logger.debug("Appended $it attributes on entity $entityId")
                if (it.notUpdated.isEmpty())
                    ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
                else
                    ResponseEntity.status(HttpStatus.MULTI_STATUS).body(it)
            }
    }

    /**
     * Implements 6.6.3.2 - Update Entity Attributes
     *
     */
    @PatchMapping("/{entityId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE])
    fun updateEntityAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @RequestBody body: Mono<String>
    ): Mono<ResponseEntity<*>> {
        val contextLink = extractContextFromLinkHeader(httpHeaders.getOrEmpty("Link"))

        return body
            .doOnNext {
                if (!entityService.exists(entityId)) throw ResourceNotFoundException("Entity $entityId does not exist")
            }
            .map {
                entityService.updateEntityAttributes(entityId, it, contextLink)
            }
            .map {
                logger.debug("Update $it attributes on entity $entityId")
                if (it.notUpdated.isEmpty())
                    ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
                else
                    ResponseEntity.status(HttpStatus.MULTI_STATUS).body(it)
            }
    }

    /**
     * Implements 6.7.3.1 - Partial Attribute Update
     * Current implementation is basic and only update the value of a property.
     */
    @PatchMapping("/{entityId}/attrs/{attrId}", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE])
    fun partialAttributeUpdate(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @PathVariable attrId: String,
        @RequestBody body: Mono<String>
    ): Mono<ResponseEntity<*>> {
        val contextLink = extractContextFromLinkHeader(httpHeaders.getOrEmpty("Link"))

        return body
            .map {
                entityService.updateEntityAttribute(entityId, attrId, it, contextLink)
            }
            .map {
                ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
            }
    }

    /**
     * Implements 6.5.3.2 - Delete Entity
     */
    @DeleteMapping("/{entityId}")
    fun delete(@PathVariable entityId: String): Mono<ResponseEntity<*>> {
        return entityId.toMono()
            .map {
                entityService.deleteEntity(entityId)
            }
            .map {
                if (it.first >= 1)
                    ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
                else
                    ResponseEntity.status(HttpStatus.NOT_FOUND).build<String>()
            }
    }
}
