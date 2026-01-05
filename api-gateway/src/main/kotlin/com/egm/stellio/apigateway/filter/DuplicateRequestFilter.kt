package com.egm.stellio.apigateway.filter

import org.springframework.cloud.gateway.filter.GatewayFilter
import org.springframework.cloud.gateway.filter.GatewayFilterChain
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono
import java.net.URI

class DuplicateRequestFilter(private val secondaryUri: String) : GatewayFilter {

    override fun filter(exchange: ServerWebExchange, chain: GatewayFilterChain): Mono<Void> {
        // The body can be read only once
        // This is fine for now as we are only implementing the "Delete and Reload @context" endpoint
        // If we want to add the "Add @Context" endpoint later, we'll need to add some cache mechanism

        WebClient.create()
            .method(exchange.request.method)
            .uri(URI.create(secondaryUri + exchange.request.path))
            .headers { httpHeaders -> httpHeaders.putAll(exchange.request.headers) }
            .retrieve()
            .bodyToMono(Void::class.java)
            .subscribe()

        return chain.filter(exchange)
    }
}
