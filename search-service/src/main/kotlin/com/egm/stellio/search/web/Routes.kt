package com.egm.stellio.search.web

import com.egm.stellio.shared.util.httpRequestPreconditions
import com.egm.stellio.shared.util.transformErrorResponse
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.server.router

@Configuration
class Routes(
    private val temporalEntityHandler: TemporalEntityHandler
) {

    @Bean
    fun router() = router {
        filter { request, next ->
            httpRequestPreconditions(request, next)
        }
        "/ngsi-ld/v1/temporal/entities".nest {
            GET("/{entityId}", temporalEntityHandler::getForEntity)
            POST("/{entityId}/attrs", temporalEntityHandler::addAttrs)
        }
        onError<Throwable> { throwable, request ->
            transformErrorResponse(throwable, request)
        }
    }
}
