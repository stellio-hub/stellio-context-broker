package com.egm.stellio.shared.config

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.http.HttpMethod
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.web.server.SecurityWebFilterChain

@Configuration
@Profile("!test")
@ConditionalOnProperty("application.authentication.enabled")
@ConditionalOnProperty(
    "application.authentication.allow-public-permission",
    havingValue = "true",
    matchIfMissing = false
)
class WebSecurityWithPublicReadConfig(
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
                exchanges.pathMatchers(HttpMethod.GET, "/ngsi-ld/v1/entities/**").permitAll()
                exchanges.pathMatchers(HttpMethod.POST, "/ngsi-ld/v1/entities/entityOperations/query").permitAll()
                exchanges.pathMatchers(HttpMethod.GET, "/ngsi-ld/v1/temporal/entities/**").permitAll()
                exchanges.pathMatchers(HttpMethod.POST, "/ngsi-ld/v1/temporal/entities/entityOperations/query")
                    .permitAll()
                exchanges.pathMatchers("/**").authenticated()
            }
            .oauth2ResourceServer { oauth2ResourceServer ->
                oauth2ResourceServer.authenticationManagerResolver(tenantAuthenticationManagerResolver)
            }

        return http.build()
    }
}
