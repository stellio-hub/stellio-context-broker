package com.egm.datahub.context.subscription.web

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.router

@Configuration
class Routes(
    private val subscriptionHandler: SubscriptionHandler
) {

    @Bean
    fun router() = router {
        (accept(MediaType.valueOf("application/ld+json")) and "/ngsi-ld/v1")
                .nest {
                    "/subscriptions".nest {
                        POST("", subscriptionHandler::create)
                        DELETE("/{subscriptionId}", subscriptionHandler::delete)
                    }
                }
    }
}
