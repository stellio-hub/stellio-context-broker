package com.egm.stellio.shared.util.config

import com.egm.stellio.shared.util.httpRequestPreconditions
import com.egm.stellio.shared.util.transformErrorResponse
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.http.HttpStatus
import org.springframework.web.reactive.function.server.*

@TestConfiguration
class RouterConfiguration {

    @Bean
    fun router() = router {
        filter { request, next ->
            httpRequestPreconditions(request, next)
        }
        "/router".nest {
            GET("/mockkedroute") { status(HttpStatus.OK).build() }
            POST("/mockkedroute") { status(HttpStatus.CREATED).build() }
            PATCH("/mockkedroute") { status(HttpStatus.NO_CONTENT).build() }
        }
        onError<Throwable> { throwable, request ->
            transformErrorResponse(throwable, request)
        }
    }
}