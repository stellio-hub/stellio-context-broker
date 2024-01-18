package com.egm.stellio.shared.web

import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.NonexistentTenantResponse
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.core.io.buffer.DefaultDataBufferFactory
import org.springframework.http.HttpHeaders.CONTENT_TYPE
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

const val NGSILD_TENANT_HEADER = "NGSILD-Tenant"
const val DEFAULT_TENANT_NAME = "urn:ngsi-ld:tenant:default"

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class TenantWebFilter(
    private val applicationProperties: ApplicationProperties
) : WebFilter {

    private lateinit var tenantsNames: List<String>

    private val logger = LoggerFactory.getLogger(javaClass)

    @PostConstruct
    fun initializeTenantsNames() {
        tenantsNames = applicationProperties.tenants.map { it.name }
    }

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val tenantNameFromHeader = exchange.request.headers[NGSILD_TENANT_HEADER]?.first()
        if (tenantNameFromHeader != null)
            exchange.response.headers.add(NGSILD_TENANT_HEADER, tenantNameFromHeader)

        val tenantName = tenantNameFromHeader ?: DEFAULT_TENANT_NAME
        if (!tenantsNames.contains(tenantName)) {
            logger.error("Unknown tenant requested: $tenantName")
            exchange.response.setStatusCode(HttpStatus.NOT_FOUND)
            exchange.response.headers[CONTENT_TYPE] = MediaType.APPLICATION_JSON_VALUE
            val errorResponse = serializeObject(NonexistentTenantResponse("Tenant $tenantName does not exist"))
            return exchange.response.writeWith(
                Flux.just(DefaultDataBufferFactory().wrap(errorResponse.toByteArray()))
            )
        }

        return chain
            .filter(exchange)
            .contextWrite { context -> context.put(NGSILD_TENANT_HEADER, tenantName) }
    }
}
