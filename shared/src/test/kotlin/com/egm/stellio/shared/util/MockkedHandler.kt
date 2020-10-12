package com.egm.stellio.shared.util

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

    @PostMapping("/validate-json-ld")
    fun validateJsonLd(@RequestBody body: Mono<String>): Mono<ResponseEntity<*>> {
        return body.map {
            JsonLdUtils.expandJsonLdEntity(it, listOf())
        }.map {
            ResponseEntity.status(HttpStatus.CREATED).build<String>()
        }
    }

    @PostMapping("/validate-json-ld-fragment")
    fun validateJsonLdFragment(@RequestBody body: Mono<String>): Mono<ResponseEntity<*>> {
        return body.map {
            JsonLdUtils.expandJsonLdFragment(it, listOf())
        }.map {
            ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
        }
    }
}
