package com.egm.stellio.shared.web

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
            else -> chain.filter(exchange)
        }
    }

    private fun httpRequestPreconditions(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        if (exchange.request.headers.contentLength == -1L)
            throw ResponseStatusException(HttpStatus.LENGTH_REQUIRED)

        return chain.filter(exchange)
    }
}