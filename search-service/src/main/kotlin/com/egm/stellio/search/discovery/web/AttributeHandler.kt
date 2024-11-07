package com.egm.stellio.search.discovery.web

import arrow.core.raise.either
import com.egm.stellio.search.discovery.service.AttributeService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.decode
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
import com.egm.stellio.shared.util.toErrorResponse
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.util.Optional

@RestController
@RequestMapping("/ngsi-ld/v1/attributes")
class AttributeHandler(
    private val attributeService: AttributeService,
    private val applicationProperties: ApplicationProperties
) {
    /**
     * Implements 6.27 - Retrieve Available Attributes
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam details: Optional<Boolean>
    ): ResponseEntity<*> = either {
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
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
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
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
