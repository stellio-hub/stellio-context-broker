package com.egm.stellio.search.web

import com.egm.stellio.shared.util.getNotAllowedMethods
import com.egm.stellio.shared.util.httpRequestPreconditions
import com.egm.stellio.shared.util.transformErrorResponse
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpStatus
import org.springframework.web.reactive.function.server.ServerResponse
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

            getNotAllowedMethods().forEach {
                method(it) { ServerResponse.status(HttpStatus.METHOD_NOT_ALLOWED).build() }
            }
        }
        onError<Throwable> { throwable, request ->
            transformErrorResponse(throwable, request)
        }
    }
}
