package com.egm.stellio.search.config

import org.springframework.context.annotation.Configuration
import org.springframework.http.codec.ServerCodecConfigurer
import org.springframework.web.reactive.config.EnableWebFlux
import org.springframework.web.reactive.config.PathMatchConfigurer
import org.springframework.web.reactive.config.WebFluxConfigurer

@Configuration
@EnableWebFlux
class WebConfig(private val searchProperties: SearchProperties) : WebFluxConfigurer {

    override fun configureHttpMessageCodecs(configurer: ServerCodecConfigurer) {
        configurer.defaultCodecs().enableLoggingRequestDetails(true)
        configurer.defaultCodecs().maxInMemorySize(searchProperties.payloadMaxBodySize)
    }

    override fun configurePathMatching(configurer: PathMatchConfigurer) {
        configurer.setUseTrailingSlashMatch(true)
    }
}
