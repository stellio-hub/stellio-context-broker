package com.egm.stellio.search.web

import arrow.core.raise.either
import com.egm.stellio.search.service.AttributeService
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
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
    ): ResponseEntity<*> = either {
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val detailedRepresentation = details.orElse(false)

        val availableAttribute: Any = if (detailedRepresentation)
            attributeService.getAttributeDetails(contexts)
        else
            attributeService.getAttributeList(contexts)

        prepareGetSuccessResponseHeaders(mediaType, contexts).body(JsonUtils.serializeObject(availableAttribute))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements 6.28 - Retrieve Available Attribute Type Information
     */
    @GetMapping("/{attrId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByAttributeId(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable attrId: String
    ): ResponseEntity<*> = either {
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val expandedAttribute = expandJsonLdTerm(attrId.decode(), contexts)

        val attributeTypeInfo =
            attributeService.getAttributeTypeInfoByAttribute(expandedAttribute, contexts).bind()

        prepareGetSuccessResponseHeaders(mediaType, contexts).body(JsonUtils.serializeObject(attributeTypeInfo))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
