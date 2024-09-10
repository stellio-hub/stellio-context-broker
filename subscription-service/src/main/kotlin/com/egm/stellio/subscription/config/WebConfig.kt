package com.egm.stellio.subscription.config

import com.egm.stellio.subscription.service.websocket.EchoHandler
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.config.EnableWebFlux
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter
import java.util.*

// @example from  https://github.com/hantsy/spring-reactive-sample/blob/master/websocket/src/main/java/com/example/demo/WebConfig.java
@Configuration
@EnableWebFlux
internal class WebConfig {
    @Bean
    fun handlerMapping(objectMapper: ObjectMapper?): HandlerMapping {
        val map: MutableMap<String, WebSocketHandler?> = HashMap()
        map["/echo"] = EchoHandler()

        // 			map.put("/custom-header", new CustomHeaderHandler());
        val mapping = SimpleUrlHandlerMapping()
        mapping.urlMap = map
        return mapping
    }

    @Bean
    fun webSocketHandlerAdapter(): WebSocketHandlerAdapter {
        return WebSocketHandlerAdapter()
    }

    @Bean
    fun jackson2ObjectMapper(): ObjectMapper {
        val builder = Jackson2ObjectMapperBuilder.json()
        builder.indentOutput(true)

        return builder.build()
    }
}
