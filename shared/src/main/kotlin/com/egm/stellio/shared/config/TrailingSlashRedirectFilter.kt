package com.egm.stellio.shared.config

import org.springframework.http.server.reactive.ServerHttpRequest
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono

@Component
class TrailingSlashRedirectFilter : WebFilter {

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val request: ServerHttpRequest = exchange.request
        val path: String = request.path.value()
        if (path.endsWith("/")) {
            val newPath = path.removeSuffix("/")
            val newRequest: ServerHttpRequest = request.mutate().path(newPath).build()
            return chain.filter(exchange.mutate().request(newRequest).build())
        }
        return chain.filter(exchange)
    }
}
