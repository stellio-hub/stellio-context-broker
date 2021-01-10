package com.egm.stellio.entity.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.web.server.SecurityWebFilterChain
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.reactive.CorsConfigurationSource
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource

@Configuration
class WebSecurityConfig {

    @Value("\${application.cors.allowed_origins}")
    val allowedOrigins: List<String>? = null
    val allowedMethods = listOf("GET", "POST", "PATCH", "DELETE")

    @Bean
    @ConditionalOnProperty("application.authentication.enabled")
    fun springSecurityFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
        http
            // disable CSRF as it does not fit with an HTTP REST API
            .csrf().disable()
            // WARNING: this will allow access to everyone to enabled actuator endpoints
            // by default, only health endpoint is activated, be careful when activating other ones
            .authorizeExchange().pathMatchers("/actuator/**").permitAll().and()
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
    fun corsConfigurationSource(): CorsConfigurationSource {
        val corsConfig = CorsConfiguration()
        corsConfig.allowedOrigins = allowedOrigins
        corsConfig.allowedMethods = allowedMethods
        corsConfig.addAllowedHeader("*")
        val source = UrlBasedCorsConfigurationSource()
        source.registerCorsConfiguration("/ngsi-ld/v1/**", corsConfig)
        return source
    }
}
