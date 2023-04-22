package com.egm.stellio.search.config

import com.egm.stellio.shared.model.NonexistentTenantResponse
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import org.slf4j.LoggerFactory
import org.springframework.core.io.buffer.DefaultDataBufferFactory
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Component
class TenantCheckWebFilter(
    private val applicationProperties: ApplicationProperties
) : WebFilter {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        return Mono.deferContextual { contextView ->
            val tenantUri = contextView.getOrDefault(NGSILD_TENANT_HEADER, DEFAULT_TENANT_NAME)!!
            if (!applicationProperties.tenants.map { it.uri.toString() }.contains(tenantUri)) {
                logger.error("Unknown tenant requested: $tenantUri")
                exchange.response.writeWith(
                    Flux.just(
                        DefaultDataBufferFactory().wrap(
                            JsonUtils.serializeObject(NonexistentTenantResponse("Tenant $tenantUri does not exist"))
                                .toByteArray()
                        )
                    )
                )
            } else
                chain.filter(exchange)
        }
    }
}
