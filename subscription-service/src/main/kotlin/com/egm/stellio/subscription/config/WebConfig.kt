package com.egm.stellio.subscription.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.config.CorsRegistry
import org.springframework.web.reactive.config.EnableWebFlux
import org.springframework.web.reactive.config.WebFluxConfigurer

@Configuration
@EnableWebFlux
class WebConfig : WebFluxConfigurer {

    @Value("\${application.cors.allowed_origins}")
    val allowedOrigins: List<String>? = null

    override fun addCorsMappings(registry: CorsRegistry) {
        allowedOrigins?.let { registry.addMapping("/ngsi-ld/v1/**").allowedOrigins(*it.toTypedArray()) }
    }
}
