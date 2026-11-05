package com.egm.stellio.shared.web

import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.server.ResponseStatusException
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono

@Component
class CustomWebFilter : WebFilter {

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        return when (exchange.request.method) {
            HttpMethod.POST -> httpRequestPreconditions(exchange, chain)
            HttpMethod.PATCH -> httpRequestPreconditions(exchange, chain)
            HttpMethod.HEAD -> headResponseStatus(exchange, chain)
            else -> chain.filter(exchange)
        }
    }

    // A HEAD request is handled by the GET handler (body omitted by Spring), but it shall return 204 on success
    // (TS 104 176 clause 5). A 204 response must not carry a Content-Length (RFC 9110).
    private fun headResponseStatus(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val response = exchange.response
        response.beforeCommit {
            if (response.statusCode == HttpStatus.OK) {
                response.statusCode = HttpStatus.NO_CONTENT
                response.headers.remove(HttpHeaders.CONTENT_LENGTH)
            }
            Mono.empty()
        }
        return chain.filter(exchange)
    }

    private fun httpRequestPreconditions(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        if (exchange.request.headers.contentLength == -1L)
            throw ResponseStatusException(HttpStatus.LENGTH_REQUIRED)

        return chain.filter(exchange)
    }
}
