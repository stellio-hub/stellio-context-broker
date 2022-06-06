package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.extractContextFromInput
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import com.egm.stellio.shared.web.ExceptionHandler as customExceptionHandler

@RestController
@RequestMapping("/router/mockkedroute")
class MockkedHandler : customExceptionHandler() {

    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun post() = ResponseEntity.status(HttpStatus.CREATED).build<String>()

    @PostMapping("/validate-json-ld-fragment")
    fun validateJsonLdFragment(@RequestBody body: Mono<String>): Mono<ResponseEntity<*>> {
        return body.map {
            val input = it.deserializeAsMap()
            expandJsonLdFragment(input, extractContextFromInput(input))
        }.map {
            ResponseEntity.status(HttpStatus.CREATED).build<String>()
        }
    }
}
