package com.egm.stellio.shared.config

import com.egm.stellio.shared.model.NonexistentTenantException
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import org.springframework.beans.factory.InitializingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.security.authentication.ReactiveAuthenticationManager
import org.springframework.security.authentication.ReactiveAuthenticationManagerResolver
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoders
import org.springframework.security.oauth2.server.resource.authentication.JwtReactiveAuthenticationManager
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono

@Component
@ConditionalOnProperty("application.authentication.enabled")
class TenantAuthenticationManagerResolver(
    private val applicationProperties: ApplicationProperties
) : ReactiveAuthenticationManagerResolver<ServerWebExchange>, InitializingBean {

    private val authenticationManagers = mutableMapOf<String, JwtReactiveAuthenticationManager>()

    override fun afterPropertiesSet() {
        applicationProperties.tenants.forEach { tenantConfiguration ->
            val jwtDecoder = ReactiveJwtDecoders.fromIssuerLocation(tenantConfiguration.issuer)
            val jwtAuthenticationManager = JwtReactiveAuthenticationManager(jwtDecoder)
            authenticationManagers[tenantConfiguration.name] = jwtAuthenticationManager
        }
    }

    override fun resolve(exchange: ServerWebExchange): Mono<ReactiveAuthenticationManager> {
        val tenantName = exchange.request.headers[NGSILD_TENANT_HEADER]?.first() ?: DEFAULT_TENANT_NAME

        return authenticationManagers[tenantName]?.let {
            Mono.just(it)
        } ?: Mono.error(NonexistentTenantException("Tenant $tenantName does not exist"))
    }
}
