package com.egm.stellio.search.web

import arrow.core.raise.either
import com.egm.stellio.search.service.EntityTypeService
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.util.Optional

@RestController
@RequestMapping("/ngsi-ld/v1/types")
class EntityTypeHandler(
    private val entityTypeService: EntityTypeService
) {

    /**
     * Implements 6.25 - Retrieve Available Entity Types
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getTypes(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam details: Optional<Boolean>
    ): ResponseEntity<*> = either {
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val detailedRepresentation = details.orElse(false)

        val availableEntityTypes: Any = if (detailedRepresentation)
            entityTypeService.getEntityTypes(contexts)
        else
            entityTypeService.getEntityTypeList(contexts)

        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .body(JsonUtils.serializeObject(availableEntityTypes))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.26 - Retrieve Available Entity Type Information
     */
    @GetMapping("/{type}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByType(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable type: String
    ): ResponseEntity<*> = either {
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val expandedType = expandJsonLdTerm(type.decode(), contexts)

        val entityTypeInfo = entityTypeService.getEntityTypeInfoByType(expandedType, contexts).bind()

        prepareGetSuccessResponseHeaders(mediaType, contexts).body(JsonUtils.serializeObject(entityTypeInfo))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
