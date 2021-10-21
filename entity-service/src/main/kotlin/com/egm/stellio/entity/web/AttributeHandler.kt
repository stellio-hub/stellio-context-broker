package com.egm.stellio.entity.web

import com.egm.stellio.entity.service.AttributeService
import com.egm.stellio.shared.util.*
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.util.Optional

@RestController
@RequestMapping("/ngsi-ld/v1/attributes")
class AttributeHandler(
    private val attributeService: AttributeService
) {
    /**
     * Implements 6.27 - Retrieve Available Attributes
     */

    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam details: Optional<Boolean>
    ): ResponseEntity<*> {
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val detailedRepresentation = details.orElse(false)

        val availableEntityTypes: Any = if (detailedRepresentation)
            attributeService.getAttributeDetails(listOf(contextLink))
        else
            attributeService.getAttributeList(listOf(contextLink))

        return buildGetSuccessResponse(mediaType, contextLink)
            .body(JsonUtils.serializeObject(availableEntityTypes))
    }
}
