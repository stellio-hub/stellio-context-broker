package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.extractContextFromInput
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import com.egm.stellio.shared.web.ExceptionHandler as customExceptionHandler

@RestController
@RequestMapping("/router/mockkedroute")
class MockkedHandler : customExceptionHandler() {

    @PostMapping(consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    fun post() = ResponseEntity.status(HttpStatus.CREATED).build<String>()

    @PostMapping("/validate-json-ld-fragment")
    suspend fun validateJsonLdFragment(@RequestBody body: Mono<String>): ResponseEntity<*> {
        val payload = body.awaitFirst().deserializeAsMap()
        expandJsonLdFragment(payload, extractContextFromInput(payload))
        return ResponseEntity.status(HttpStatus.CREATED).build<String>()
    }

    @GetMapping("/ok")
    suspend fun ok(): ResponseEntity<*> = ResponseEntity.status(HttpStatus.OK).build<String>()
}
