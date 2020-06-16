package com.egm.stellio.subscription.config

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.web.server.SecurityWebFilterChain

@Configuration
class WebSecurityConfig {

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
}
