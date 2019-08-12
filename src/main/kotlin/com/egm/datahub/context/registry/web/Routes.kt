package com.egm.datahub.context.registry.web

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.router

@Configuration
class Routes(
        private val entityHandler: EntityHandler
) {

    @Bean
    fun router() = router {
        (accept(MediaType.valueOf("application/ld+json")) and "/ngsi-ld/v1").nest {
            "/entities".nest {
                POST("", entityHandler::create)
                GET("{entityId}", entityHandler::getById)
                POST("/play", entityHandler::parseAndPlay)
            }
        }
    }
}