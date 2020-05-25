package com.egm.stellio.search.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoders
import org.springframework.security.web.server.SecurityWebFilterChain

@Configuration
class WebSecurityConfig {
    @Value("\${spring.security.oauth2.resourceserver.jwt.issuer-uri}")
    val issuerUri: String? = null

    @Bean
    @ConditionalOnProperty("application.authentication.enabled")
    fun springSecurityFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
        http
            // disable CSRF as it does not fit with an HTTP REST API
            .csrf().disable()
            // tweak Actuator health endpoint security rules to grant access to anonymous users
            .authorizeExchange().pathMatchers("/actuator/health").permitAll().and()
            .authorizeExchange().pathMatchers("/**").authenticated().and()
            .oauth2ResourceServer().jwt()

        return http.build()
    }

    @Bean
    @ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
    fun springNoSecurityFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
        http
            // disable CSRF as it does not fit with an HTTP REST API
            .csrf().disable()
            // explicitly disable authentication to override Spring Security defaults
            .authorizeExchange().pathMatchers("/**").permitAll()

        return http.build()
    }

    @Bean
    fun jwtDecoder(): ReactiveJwtDecoder {
        return ReactiveJwtDecoders.fromOidcIssuerLocation(issuerUri)
    }
}