package com.egm.stellio.shared.web

import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JSON_MERGE_PATCH_MEDIA_TYPE
import com.egm.stellio.shared.util.isAcceptable
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
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
            HttpMethod.GET -> httpGetRequestPreconditions(exchange, chain)
            HttpMethod.POST -> httpPostRequestPreconditions(exchange, chain)
            HttpMethod.PATCH -> httpPatchRequestPreconditions(exchange, chain)
            else -> chain.filter(exchange)
        }
    }

    private fun httpGetRequestPreconditions(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val request: ServerHttpRequest = exchange.request
        val accept = request.headers.accept

        return if (accept.isNotEmpty() && !accept.isAcceptable())
            throw ResponseStatusException(HttpStatus.NOT_ACCEPTABLE)
        else
            chain.filter(exchange)
    }

    private fun httpPostRequestPreconditions(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val request: ServerHttpRequest = exchange.request
        val contentType = request.headers.contentType
        val contentLength = request.headers.contentLength

        return if (contentLength == -1L)
            throw ResponseStatusException(HttpStatus.LENGTH_REQUIRED)
        else if (contentType == null || !listOf(MediaType.APPLICATION_JSON, JSON_LD_MEDIA_TYPE).contains(MediaType(contentType.type, contentType.subtype)))
            throw ResponseStatusException(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
        else
            chain.filter(exchange)
    }

    private fun httpPatchRequestPreconditions(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val request: ServerHttpRequest = exchange.request
        val contentType = request.headers.contentType
        val contentLength = request.headers.contentLength

        return if (contentLength == -1L)
            throw ResponseStatusException(HttpStatus.LENGTH_REQUIRED)
        else if (contentType == null || !listOf(MediaType.APPLICATION_JSON, JSON_LD_MEDIA_TYPE, JSON_MERGE_PATCH_MEDIA_TYPE).contains(MediaType(contentType.type, contentType.subtype)))
            throw ResponseStatusException(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
        else
            chain.filter(exchange)
    }
}