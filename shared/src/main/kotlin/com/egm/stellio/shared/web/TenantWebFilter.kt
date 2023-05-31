package com.egm.stellio.shared.web

import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.BadRequestDataResponse
import com.egm.stellio.shared.model.NonexistentTenantResponse
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.invalidUriMessage
import com.egm.stellio.shared.util.isURI
import com.egm.stellio.shared.util.toUri
import jakarta.annotation.PostConstruct
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
import java.net.URI

const val NGSILD_TENANT_HEADER = "NGSILD-Tenant"
val DEFAULT_TENANT_URI = "urn:ngsi-ld:tenant:default".toUri()

@Component
@Order(0)
class TenantWebFilter(
    private val applicationProperties: ApplicationProperties
) : WebFilter {

    private lateinit var tenantsUris: List<URI>

    private val logger = LoggerFactory.getLogger(javaClass)

    @PostConstruct
    fun initializeTenantsUris() {
        tenantsUris = applicationProperties.tenants.map { it.uri }
    }

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val tenantUri = exchange.request.headers[NGSILD_TENANT_HEADER]?.first()
            .also {
                if (it != null)
                    exchange.response.headers.add(NGSILD_TENANT_HEADER, it)
            }?.also {
                if (!it.isURI()) {
                    logger.error("Requested tenant is not a valid URI: $it")
                    exchange.response.setStatusCode(HttpStatus.BAD_REQUEST)
                    val errorResponse = serializeObject(BadRequestDataResponse(invalidUriMessage("$it (tenant)")))
                    return exchange.response.writeWith(
                        Flux.just(DefaultDataBufferFactory().wrap(errorResponse.toByteArray()))
                    )
                } else if (!tenantsUris.contains(it.toUri())) {
                    logger.error("Unknown tenant requested: $it")
                    exchange.response.setStatusCode(HttpStatus.NOT_FOUND)
                    val errorResponse = serializeObject(NonexistentTenantResponse("Tenant $it does not exist"))
                    return exchange.response.writeWith(
                        Flux.just(DefaultDataBufferFactory().wrap(errorResponse.toByteArray()))
                    )
                }
            }?.toUri() ?: DEFAULT_TENANT_URI

        return chain
            .filter(exchange)
            .contextWrite { context -> context.put(NGSILD_TENANT_HEADER, tenantUri) }
    }
}
