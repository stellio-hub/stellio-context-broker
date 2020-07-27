package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.entity.util.decode
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.BadRequestDataResponse
import com.egm.stellio.shared.model.InternalErrorResponse
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.ApiUtils.serializeObject
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JSON_MERGE_PATCH_CONTENT_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.shared.util.NgsiLdParsingUtils.compactEntities
import com.egm.stellio.shared.util.extractContextFromLinkHeader
import com.egm.stellio.shared.web.extractSubjectOrEmpty
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.net.URI
import java.util.Optional

@RestController
@RequestMapping("/ngsi-ld/v1/entities")
class EntityHandler(
    private val entityService: EntityService,
    private val authorizationService: AuthorizationService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Implements 6.4.3.1 - Create Entity
     */
    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun create(@RequestBody body: Mono<String>): Mono<ResponseEntity<*>> {
        return extractSubjectOrEmpty().flatMap { userId ->
            if (!authorizationService.userCanCreateEntities(userId))
                throw AccessDeniedException("User forbidden to create entities")
            body.map {
                NgsiLdParsingUtils.parseEntity(it, NgsiLdParsingUtils.getContextOrThrowError(it))
            }.map {
                entityService.createEntity(it)
            }.doOnNext {
                authorizationService.createAdminLink(it.id, userId)
            }.map {
                ResponseEntity
                    .status(HttpStatus.CREATED)
                    .location(URI("/ngsi-ld/v1/entities/${it.id}"))
                    .build<String>()
            }
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

        return Mono.just(entityService.searchEntities(type, q.decode(), contextLink)).zipWith(extractSubjectOrEmpty())
            .map { entitiesAndUserId ->
                Pair(
                    entitiesAndUserId.t1, authorizationService.filterEntitiesUserCanRead(
                        entitiesAndUserId.t1.map { it.id },
                        entitiesAndUserId.t2
                    )
                )
            }.map { (searchEntities, authorizedIds) ->
                searchEntities.filter { authorizedIds.contains(it.id) }
            }.map {
                compactEntities(it)
            }.map {
                ResponseEntity.status(HttpStatus.OK).body(serializeObject(it))
            }
    }

    /**
     * Implements 6.5.3.1 - Retrieve Entity
     */
    @GetMapping("/{entityId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun getByURI(@PathVariable entityId: String): Mono<ResponseEntity<*>> {
        return extractSubjectOrEmpty().doOnNext {
            if (!entityService.exists(entityId)) throw ResourceNotFoundException("Entity Not Found")
            if (!authorizationService.userCanReadEntity(
                    entityId,
                    it
                )
            ) throw AccessDeniedException("User forbidden read access to entity $entityId")
        }.map {
            entityService.getFullEntityById(entityId)
        }.map {
            it.compact()
        }.map {
            ResponseEntity.status(HttpStatus.OK).body(serializeObject(it))
        }
    }

    /**
     * Implements 6.5.3.2 - Delete Entity
     */
    @DeleteMapping("/{entityId}")
    fun delete(@PathVariable entityId: String): Mono<ResponseEntity<*>> {
        return extractSubjectOrEmpty().doOnNext {
            if (!entityService.exists(entityId))
                throw ResourceNotFoundException("Entity Not Found")
            if (!authorizationService.userCanAdminEntity(entityId, it))
                throw AccessDeniedException("User forbidden admin access to entity $entityId")
        }.map {
            entityService.deleteEntity(entityId)
        }.map {
            if (it.first >= 1)
                ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
            else
                ResponseEntity.status(HttpStatus.NOT_FOUND).build<String>()
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
        return extractSubjectOrEmpty().doOnNext {
            if (!entityService.exists(entityId)) throw ResourceNotFoundException("Entity $entityId does not exist")
            if (!authorizationService.userCanWriteEntity(entityId, it))
                throw AccessDeniedException("User forbidden write access to entity $entityId")
        }.then(body).map {
            NgsiLdParsingUtils.expandJsonLdFragment(it, contextLink)
        }.map {
            entityService.appendEntityAttributes(entityId, it, disallowOverwrite)
        }.map {
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
    @PatchMapping(
        "/{entityId}/attrs",
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    fun updateEntityAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @RequestBody body: Mono<String>
    ): Mono<ResponseEntity<*>> {
        val contextLink = extractContextFromLinkHeader(httpHeaders.getOrEmpty("Link"))
        return extractSubjectOrEmpty().doOnNext {
            if (!entityService.exists(entityId)) throw ResourceNotFoundException("Entity $entityId does not exist")
            if (!authorizationService.userCanWriteEntity(entityId, it))
                throw AccessDeniedException("User forbidden write access to entity $entityId")
        }.then(body).map {
            entityService.updateEntityAttributes(entityId, it, contextLink)
        }.map {
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
    @PatchMapping(
        "/{entityId}/attrs/{attrId}",
        consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE, JSON_MERGE_PATCH_CONTENT_TYPE]
    )
    fun partialAttributeUpdate(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @PathVariable attrId: String,
        @RequestBody body: Mono<String>
    ): Mono<ResponseEntity<*>> {
        val contextLink = extractContextFromLinkHeader(httpHeaders.getOrEmpty("Link"))
        return extractSubjectOrEmpty().doOnNext {
            if (!entityService.exists(entityId)) throw ResourceNotFoundException("Entity $entityId does not exist")
            if (!authorizationService.userCanWriteEntity(entityId, it))
                throw AccessDeniedException("User forbidden write access to entity $entityId")
        }.then(body).map {
            entityService.updateEntityAttribute(entityId, attrId, it, contextLink)
        }.map {
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }
    }

    /**
     * Implements 6.7.3.2 - Delete Entity Attribute
     * Current implementation is basic since there is no support for Multi-Attribute (4.5.5).
     */
    @DeleteMapping("/{entityId}/attrs/{attrId}")
    fun deleteEntityAttribute(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @PathVariable attrId: String
    ): Mono<ResponseEntity<*>> {
        val contextLink = extractContextFromLinkHeader(httpHeaders.getOrEmpty("Link"))
        return extractSubjectOrEmpty().doOnNext {
            if (!entityService.exists(entityId)) throw ResourceNotFoundException("Entity $entityId does not exist")
            if (!authorizationService.userCanWriteEntity(entityId, it))
                throw AccessDeniedException("User forbidden write access to entity $entityId")
        }.map {
            entityService.deleteEntityAttribute(entityId, attrId, contextLink)
        }.map {
            if (it)
                ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
            else
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).contentType(MediaType.APPLICATION_JSON)
                    .body(InternalErrorResponse("An error occurred while deleting $attrId from $entityId"))
        }
    }
}
