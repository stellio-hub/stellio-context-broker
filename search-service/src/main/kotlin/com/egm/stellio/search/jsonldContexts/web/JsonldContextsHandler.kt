package com.egm.stellio.search.jsonldContexts.web

import arrow.core.raise.either
import com.egm.stellio.shared.queryparameter.AllowedParameters
import com.egm.stellio.shared.queryparameter.QP
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.web.BaseHandler
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.net.URI

@RestController
@RequestMapping("/ngsi-ld/v1/jsonldContexts")
class JsonldContextsHandler : BaseHandler() {

    /**
     * Implements 6.30.3.2 - Delete and Reload @Context
     */
    @DeleteMapping("/{contextId}")
    suspend fun deleteAndReload(
        @PathVariable contextId: URI,
        @AllowedParameters(notImplemented = [QP.RELOAD])
        @RequestParam queryParams: MultiValueMap<String, String>
    ): ResponseEntity<*> = either {
        JsonLdUtils.deleteAndReload(contextId).bind()

        ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }.fold(
        { it.toErrorResponse() },
        { it }
    )
}
