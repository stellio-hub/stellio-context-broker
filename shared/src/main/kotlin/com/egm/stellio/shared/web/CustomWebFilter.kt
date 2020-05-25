package com.egm.stellio.shared.web

import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.server.reactive.ServerHttpRequest
import org.springframework.stereotype.Component
import org.springframework.web.server.ResponseStatusException
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono

@Component
class CustomWebFilter : WebFilter {

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val request: ServerHttpRequest = exchange.request
        return when (request.method) {
            HttpMethod.POST -> httpPostRequestPreconditions(exchange, chain)
            HttpMethod.PATCH -> httpPatchRequestPreconditions(exchange, chain)
            else -> chain.filter(exchange)
        }
    }

    private fun httpPostRequestPreconditions(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val request: ServerHttpRequest = exchange.request
        val contentLength = request.headers.contentLength

        if (contentLength == -1L)
            throw ResponseStatusException(HttpStatus.LENGTH_REQUIRED)

        return chain.filter(exchange)
    }

    private fun httpPatchRequestPreconditions(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val request: ServerHttpRequest = exchange.request
        val contentLength = request.headers.contentLength

        if (contentLength == -1L)
            throw ResponseStatusException(HttpStatus.LENGTH_REQUIRED)

        return chain.filter(exchange)
    }
}