package com.egm.stellio.subscription.config

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder
import org.springframework.web.reactive.config.EnableWebFlux

// @example from  https://github.com/hantsy/spring-reactive-sample/blob/master/websocket/src/main/java/com/example/demo/WebConfig.java
@Configuration
@EnableWebFlux
internal class WebConfig {
    @Bean
    fun jackson2ObjectMapper(): ObjectMapper {
        val builder = Jackson2ObjectMapperBuilder.json()
        builder.indentOutput(true)

        return builder.build()
    }
}
