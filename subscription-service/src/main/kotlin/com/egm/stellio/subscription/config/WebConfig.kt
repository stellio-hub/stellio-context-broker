package com.egm.stellio.subscription.config

import org.slf4j.LoggerFactory
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

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun addCorsMappings(registry: CorsRegistry) {
        allowedOrigins?.let {
            logger.info("Enabling Cross Origin Requests for origins: $it")
            registry.addMapping("/ngsi-ld/v1/**").allowedOrigins(*it.toTypedArray())
        }
    }
}
