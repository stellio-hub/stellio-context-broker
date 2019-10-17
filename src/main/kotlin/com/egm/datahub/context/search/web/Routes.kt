package com.egm.datahub.context.search.web

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.router

@Configuration
class Routes(
    private val statusHandler: StatusHandler
) {

    @Bean
    fun router() = router {
        (accept(MediaType.APPLICATION_JSON) and "/api").nest {
            "/status".nest {
                GET("/", statusHandler::status)
            }
        }
    }
}