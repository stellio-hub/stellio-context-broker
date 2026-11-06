package com.egm.stellio.search.discovery.web

import arrow.core.raise.either
import com.egm.stellio.search.csr.model.addWarnings
import com.egm.stellio.search.csr.service.DistributedDiscoveryService
import com.egm.stellio.search.csr.service.distributeIfNotLocal
import com.egm.stellio.search.discovery.model.merge
import com.egm.stellio.search.discovery.service.EntityTypeService
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
@RequestMapping("/ngsi-ld/v1/types")
@Validated
class EntityTypeHandler(
    private val entityTypeService: EntityTypeService,
    private val distributedDiscoveryService: DistributedDiscoveryService,
    private val applicationProperties: ApplicationProperties
) {

    /**
     * Implements TS 104 176 §7.15 - Retrieve Available Entity Types
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getTypes(
        @RequestHeader httpHeaders: HttpHeaders,
        @AllowedParameters(implemented = [QP.DETAILS, QP.LOCAL], notImplemented = [QP.VIA])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val details = queryParams.getFirst(QP.DETAILS.key)?.toBoolean() ?: false
        val local = queryParams.getFirst(QP.LOCAL.key)?.toBoolean() ?: false

        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()

        val (warnings, availableEntityTypes) = if (details) {
            val localEntityTypes = entityTypeService.getEntityTypes(contexts)
            distributeIfNotLocal(local) {
                distributedDiscoveryService.distributeRetrieveEntityTypeDetails(httpHeaders, contexts)
            }.let { (warnings, remoteTypes) -> warnings to (listOf(localEntityTypes) + remoteTypes).merge() }
        } else {
            val localEntityTypeList = entityTypeService.getEntityTypeList(contexts)
            distributeIfNotLocal(local) {
                distributedDiscoveryService.distributeRetrieveEntityTypes(httpHeaders, contexts)
            }.let { (warnings, remoteLists) -> warnings to (listOf(localEntityTypeList) + remoteLists).merge() }
        }

        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .body(JsonUtils.serializeObject(availableEntityTypes))
            .addWarnings(warnings)
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    /**
     * Implements TS 104 176 §7.16 - Retrieve Available Entity Type Information
     */
    @GetMapping("/{type}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getByType(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable type: String,
        @AllowedParameters(implemented = [QP.LOCAL], notImplemented = [QP.VIA])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val local = queryParams.getFirst(QP.LOCAL.key)?.toBoolean() ?: false
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).bind()
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val expandedType = expandJsonLdTerm(type.decode(), contexts)

        val localEntityTypeInfo = entityTypeService.getEntityTypeInfoByType(expandedType, contexts)
        val (warnings, remoteEntityTypeInfos) = distributeIfNotLocal(local) {
            distributedDiscoveryService.distributeRetrieveEntityTypeInfo(expandedType, httpHeaders, contexts)
        }

        // when nothing was found, the local result is necessarily a ResourceNotFoundException
        val response = either {
            (listOfNotNull(localEntityTypeInfo.getOrNull()) + remoteEntityTypeInfos).merge()
                ?: localEntityTypeInfo.bind()
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
