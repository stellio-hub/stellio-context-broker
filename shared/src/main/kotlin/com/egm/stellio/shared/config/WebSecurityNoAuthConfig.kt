package com.egm.stellio.shared.config

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.web.server.SecurityWebFilterChain

@Configuration
@ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
class WebSecurityNoAuthConfig {

    @Bean
    fun springNoSecurityFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
        http
            // disable CSRF as it does not fit with an HTTP REST API
            .csrf { csrf -> csrf.disable() }
            // explicitly disable authentication to override Spring Security defaults
            .authorizeExchange { exchanges ->
                exchanges.pathMatchers("/**").permitAll()
            }

        return http.build()
    }
}
