package com.egm.stellio.shared.config

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.web.server.SecurityWebFilterChain

@Configuration
@ConditionalOnProperty("application.authentication.enabled")
class WebSecurityConfig(
    private val tenantAuthenticationManagerResolver: TenantAuthenticationManagerResolver
) {

    @Bean
    fun springSecurityFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
        http
            // disable CSRF as it does not fit with an HTTP REST API
            .csrf { csrf -> csrf.disable() }
            // WARNING: this will allow access to everyone to enabled actuator endpoints
            // by default, only health endpoint is activated, be careful when activating other ones
            .authorizeExchange { exchanges ->
                exchanges.pathMatchers("/actuator/**").permitAll()
                exchanges.pathMatchers("/**").authenticated()
            }
            .oauth2ResourceServer { oauth2ResourceServer ->
                oauth2ResourceServer.authenticationManagerResolver(tenantAuthenticationManagerResolver)
            }

        return http.build()
    }
}
