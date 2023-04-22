package com.egm.stellio.shared.web

import com.egm.stellio.shared.model.BadRequestDataResponse
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.invalidUriMessage
import com.egm.stellio.shared.util.isURI
import org.slf4j.LoggerFactory
import org.springframework.core.annotation.Order
import org.springframework.core.io.buffer.DefaultDataBufferFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

const val NGSILD_TENANT_HEADER = "NGSILD-Tenant"
const val DEFAULT_TENANT_NAME = "urn:ngsi-ld:tenant:default"

@Component
@Order(0)
class TenantWebFilter : WebFilter {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val tenant = exchange.request.headers[NGSILD_TENANT_HEADER]?.first()
            .also {
                logger.debug("Request is targeting tenant $it")
                if (it != null)
                    exchange.response.headers.add(NGSILD_TENANT_HEADER, it)
            }?.also {
                if (!it.isURI()) {
                    exchange.response.setStatusCode(HttpStatus.BAD_REQUEST)
                    val errorResponse = serializeObject(BadRequestDataResponse(invalidUriMessage("$it (tenant)")))
                    return exchange.response.writeWith(
                        Flux.just(DefaultDataBufferFactory().wrap(errorResponse.toByteArray()))
                    )
                }
            } ?: DEFAULT_TENANT_NAME

        return chain
            .filter(exchange)
            .contextWrite { context -> context.put(NGSILD_TENANT_HEADER, tenant) }
    }
}
