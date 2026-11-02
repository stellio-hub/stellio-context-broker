package com.egm.stellio.search.csr.web

import arrow.core.raise.either
import com.egm.stellio.search.csr.model.ContextSourceIdentity
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.queryparameter.AllowedParameters
import com.egm.stellio.shared.util.DataTypes.convertTo
import com.egm.stellio.shared.util.DataTypes.serialize
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getTenantFromContext
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
import com.egm.stellio.shared.util.toFinalRepresentation
import com.egm.stellio.shared.web.BaseHandler
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@Validated
@RequestMapping("/ngsi-ld/v1/info")
class ContextSourceIdentityHandler(
    private val applicationProperties: ApplicationProperties
) : BaseHandler() {

    /**
     * Implements TS 104 176 §12.2.3.1 - Retrieve Context Source Identity Information
     */
    @GetMapping("/sourceIdentity", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun retrieve(
        @RequestHeader httpHeaders: HttpHeaders,
        @AllowedParameters // no query parameter is allowed
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        val mediaType = getApplicableMediaType(httpHeaders).bind()
        val contexts = listOf(applicationProperties.contexts.core)

        val contextSourceIdentity = ContextSourceIdentity.forTenant(
            applicationProperties.contextSourceAlias,
            getTenantFromContext(),
            applicationProperties.contextSourceExtras
        )

        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .body(
                serialize(
                    convertTo<Map<String, Any>>(contextSourceIdentity)
                        .plus(JSONLD_CONTEXT_KW to contexts)
                        .toFinalRepresentation(mediaType)
                )
            )
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
