package com.egm.stellio.entity.web

import com.egm.stellio.shared.util.httpRequestPreconditions
import com.egm.stellio.shared.util.transformErrorResponse
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.server.router

@Configuration
class Routes(
    private val entityHandler: EntityHandler,
    private val entityOperationHandler: EntityOperationHandler
) {

    @Bean
    fun router() = router {
        filter { request, next ->
            httpRequestPreconditions(request, next)
        }
        "/ngsi-ld/v1".nest {
            "/entities".nest {
                POST("", entityHandler::create)
                GET("", entityHandler::getEntities)
                GET("/{entityId}", entityHandler::getByURI)
                PATCH("/{entityId}/attrs/{attrId}", entityHandler::partialAttributeUpdate)
                PATCH("/{entityId}/attrs", entityHandler::updateEntityAttributes)
                POST("/{entityId}/attrs", entityHandler::appendEntityAttributes)
                DELETE("/{entityId}", entityHandler::delete)
            }
            "/entityOperations".nest {
                POST("/create", entityOperationHandler::create)
            }
        }
        onError<Throwable> { throwable, request ->
            transformErrorResponse(throwable, request)
        }
    }
}
