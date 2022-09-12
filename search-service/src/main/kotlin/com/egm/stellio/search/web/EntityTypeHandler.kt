package com.egm.stellio.search.web

import arrow.core.continuations.either
import com.egm.stellio.search.service.EntityTypeService
import com.egm.stellio.shared.model.APIException
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
    ): ResponseEntity<*> {
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val detailedRepresentation = details.orElse(false)

        return either<APIException, ResponseEntity<*>> {
            val availableEntityTypes: Any = if (detailedRepresentation)
                entityTypeService.getEntityTypes(listOf(contextLink))
            else
                entityTypeService.getEntityTypeList(listOf(contextLink))

            prepareGetSuccessResponse(mediaType, contextLink)
                .body(JsonUtils.serializeObject(availableEntityTypes))
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }

    /**
     * Implements 6.26 - Retrieve Available Entity Type Information
     */
    @GetMapping("/{type}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByType(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable type: String
    ): ResponseEntity<*> {
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val expandedType = expandJsonLdTerm(type.decode(), contextLink)

        return either<APIException, ResponseEntity<*>> {
            val entityTypeInfo = entityTypeService.getEntityTypeInfoByType(expandedType, listOf(contextLink)).bind()

            prepareGetSuccessResponse(mediaType, contextLink).body(JsonUtils.serializeObject(entityTypeInfo))
        }.fold(
            { it.toErrorResponse() },
            { it }
        )
    }
}
