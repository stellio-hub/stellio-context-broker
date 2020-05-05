package com.egm.stellio.subscription.web

import com.egm.stellio.shared.util.httpRequestPreconditions
import com.egm.stellio.shared.util.transformErrorResponse
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.server.router

@Configuration
class Routes(
    private val subscriptionHandler: SubscriptionHandler
) {

    @Bean
    fun router() = router {
        filter { request, next ->
            httpRequestPreconditions(request, next)
        }
        "/ngsi-ld/v1/subscriptions".nest {
            POST("", subscriptionHandler::create)
            GET("", subscriptionHandler::getSubscriptions)
            GET("/{subscriptionId}", subscriptionHandler::getByURI)
            PATCH("/{subscriptionId}", subscriptionHandler::update)
            DELETE("/{subscriptionId}", subscriptionHandler::delete)
        }
        onError<Throwable> { throwable, request ->
            transformErrorResponse(throwable, request)
        }
    }
}