package com.egm.datahub.context.search.web

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.router

@Configuration
class Routes(
    private val statusHandler: StatusHandler,
    private val temporalEntityHandler: TemporalEntityHandler
) {

    @Bean
    fun router() = router {
        (accept(MediaType.APPLICATION_JSON) and "/api").nest {
            "/status".nest {
                GET("/", statusHandler::status)
            }
        }
        (accept(MediaType.valueOf("application/ld+json")) and "/ngsi-ld/v1").nest {
            "/temporal/entities".nest {
                GET("/{entityId}", temporalEntityHandler::getForEntity)
                POST("/{entityId}/attrs", temporalEntityHandler::addAttrs)
            }
        }
    }
}
