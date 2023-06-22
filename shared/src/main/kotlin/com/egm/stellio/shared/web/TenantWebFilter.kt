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
import java.net.URI

const val NGSILD_TENANT_HEADER = "NGSILD-Tenant"
val DEFAULT_TENANT_URI = "urn:ngsi-ld:tenant:default".toUri()

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
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
        val tenantUriFromHeader = exchange.request.headers[NGSILD_TENANT_HEADER]?.first()
        if (tenantUriFromHeader != null)
            exchange.response.headers.add(NGSILD_TENANT_HEADER, tenantUriFromHeader)

        val tenantUri = tenantUriFromHeader ?: DEFAULT_TENANT_URI.toString()
        if (!tenantUri.isURI()) {
            logger.error("Requested tenant is not a valid URI: $tenantUri")
            exchange.response.setStatusCode(HttpStatus.BAD_REQUEST)
            exchange.response.headers[CONTENT_TYPE] = MediaType.APPLICATION_JSON_VALUE
            val errorResponse = serializeObject(BadRequestDataResponse(invalidUriMessage("$tenantUri (tenant)")))
            return exchange.response.writeWith(
                Flux.just(DefaultDataBufferFactory().wrap(errorResponse.toByteArray()))
            )
        } else if (!tenantsUris.contains(tenantUri.toUri())) {
            logger.error("Unknown tenant requested: $tenantUri")
            exchange.response.setStatusCode(HttpStatus.NOT_FOUND)
            exchange.response.headers[CONTENT_TYPE] = MediaType.APPLICATION_JSON_VALUE
            val errorResponse = serializeObject(NonexistentTenantResponse("Tenant $tenantUri does not exist"))
            return exchange.response.writeWith(
                Flux.just(DefaultDataBufferFactory().wrap(errorResponse.toByteArray()))
            )
        }

        return chain
            .filter(exchange)
            .contextWrite { context -> context.put(NGSILD_TENANT_HEADER, tenantUri.toUri()) }
    }
}
