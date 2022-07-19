package com.egm.stellio.entity.web

import com.egm.stellio.entity.service.EntityTypeService
import com.egm.stellio.shared.model.ResourceNotFoundException
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

        val availableEntityTypes: Any = if (detailedRepresentation)
            entityTypeService.getEntityTypes(listOf(contextLink))
        else
            entityTypeService.getEntityTypeList(listOf(contextLink))

        return prepareGetSuccessResponse(mediaType, contextLink)
            .body(JsonUtils.serializeObject(availableEntityTypes))
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

        val entityTypeInfo = entityTypeService.getEntityTypeInfo(expandedType, listOf(contextLink))
            ?: throw ResourceNotFoundException("No entities found for type $expandedType")

        return prepareGetSuccessResponse(mediaType, contextLink)
            .body(JsonUtils.serializeObject(entityTypeInfo))
    }
}
