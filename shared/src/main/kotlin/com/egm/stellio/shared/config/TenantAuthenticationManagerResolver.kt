package com.egm.stellio.shared.config

import com.egm.stellio.shared.model.NonexistentTenantException
import com.egm.stellio.shared.web.DEFAULT_TENANT_URI
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import jakarta.annotation.PostConstruct
import org.springframework.security.authentication.ReactiveAuthenticationManager
import org.springframework.security.authentication.ReactiveAuthenticationManagerResolver
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoders
import org.springframework.security.oauth2.server.resource.authentication.JwtReactiveAuthenticationManager
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono

@Component
class TenantAuthenticationManagerResolver(
    private val applicationProperties: ApplicationProperties
) : ReactiveAuthenticationManagerResolver<ServerWebExchange> {

    private val authenticationManagers = mutableMapOf<String, JwtReactiveAuthenticationManager>()

    @PostConstruct
    fun initializeJwtAuthenticationManagers() {
        applicationProperties.tenants.forEach { tenantConfiguration ->
            val jwtDecoder = ReactiveJwtDecoders.fromIssuerLocation(tenantConfiguration.issuer)
            val jwtAuthenticationManager = JwtReactiveAuthenticationManager(jwtDecoder)
            authenticationManagers[tenantConfiguration.uri.toString()] = jwtAuthenticationManager
        }
    }

    override fun resolve(exchange: ServerWebExchange): Mono<ReactiveAuthenticationManager> {
        val tenantUri = exchange.request.headers[NGSILD_TENANT_HEADER]?.first() ?: DEFAULT_TENANT_URI.toString()

        return authenticationManagers[tenantUri]?.let {
            Mono.just(it)
        } ?: Mono.error(NonexistentTenantException("Tenant $tenantUri does not exist"))
    }
}
