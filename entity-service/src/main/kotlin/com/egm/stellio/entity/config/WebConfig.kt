package com.egm.stellio.entity.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.http.codec.ServerCodecConfigurer
import org.springframework.web.reactive.config.CorsRegistry
import org.springframework.web.reactive.config.EnableWebFlux
import org.springframework.web.reactive.config.WebFluxConfigurer

@Profile("!test")
@Configuration
@EnableWebFlux
class WebConfig : WebFluxConfigurer {

    @Value("\${allowed.origins}")
    val allowedOrigins: List<String>? = null

    override fun configureHttpMessageCodecs(configurer: ServerCodecConfigurer) {
        configurer.defaultCodecs().enableLoggingRequestDetails(true)
    }

    override fun addCorsMappings(registry: CorsRegistry) {
        allowedOrigins?.let { registry.addMapping("/ngsi-ld/v1/**").allowedOrigins(*it.toTypedArray()) }
    }
}
