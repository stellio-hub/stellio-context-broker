package com.egm.stellio.search.discovery.web

import arrow.core.raise.either
import com.egm.stellio.search.csr.model.addWarnings
import com.egm.stellio.search.csr.service.DistributedDiscoveryService
import com.egm.stellio.search.csr.service.distributeIfNotLocal
import com.egm.stellio.search.discovery.model.merge
import com.egm.stellio.search.discovery.service.AttributeService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.queryparameter.AllowedParameters
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.decode
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/ngsi-ld/v1/attributes")
@Validated
class AttributeHandler(
    private val attributeService: AttributeService,
    private val distributedDiscoveryService: DistributedDiscoveryService,
    private val applicationProperties: ApplicationProperties
) {
    /**
     * Implements TS 104 176 §7.17 - Retrieve Available Attributes
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @AllowedParameters(implemented = [QP.DETAILS, QP.LOCAL], notImplemented = [QP.VIA])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val details = queryParams.getFirst(QP.DETAILS.key)?.toBoolean() ?: false
        val local = queryParams.getFirst(QP.LOCAL.key)?.toBoolean() ?: false

        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()

        val (warnings, availableAttributes) = if (details) {
            val localAttributes = attributeService.getAttributeDetails(contexts)
            distributeIfNotLocal(local) {
                distributedDiscoveryService.distributeRetrieveAttributeDetails(httpHeaders, contexts)
            }.let { (warnings, remoteAttributes) -> warnings to (listOf(localAttributes) + remoteAttributes).merge() }
        } else {
            val localAttributeList = attributeService.getAttributeList(contexts)
            distributeIfNotLocal(local) {
                distributedDiscoveryService.distributeRetrieveAttributes(httpHeaders, contexts)
            }.let { (warnings, remoteLists) -> warnings to (listOf(localAttributeList) + remoteLists).merge() }
        }

        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .body(JsonUtils.serializeObject(availableAttributes))
            .addWarnings(warnings)
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements TS 104 176 §7.18 - Retrieve Available Attribute Type Information
     */
    @GetMapping("/{attrId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByAttributeId(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable attrId: String,
        @AllowedParameters(implemented = [QP.LOCAL], notImplemented = [QP.VIA])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val local = queryParams.getFirst(QP.LOCAL.key)?.toBoolean() ?: false
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val expandedAttribute = expandJsonLdTerm(attrId.decode(), contexts)

        val localAttributeTypeInfo = attributeService.getAttributeTypeInfoByAttribute(expandedAttribute, contexts)
        val (warnings, remoteAttributeTypeInfos) = distributeIfNotLocal(local) {
            distributedDiscoveryService.distributeRetrieveAttributeTypeInfo(expandedAttribute, httpHeaders, contexts)
        }

        // when nothing was found, the local result is necessarily a ResourceNotFoundException
        val response = either {
            (listOfNotNull(localAttributeTypeInfo.getOrNull()) + remoteAttributeTypeInfos).merge()
                ?: localAttributeTypeInfo.bind()
        }.fold(
            { it.toErrorResponse() },
            { prepareGetSuccessResponseHeaders(mediaType, contexts).body(JsonUtils.serializeObject(it)) }
        )

        response.addWarnings(warnings)
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
