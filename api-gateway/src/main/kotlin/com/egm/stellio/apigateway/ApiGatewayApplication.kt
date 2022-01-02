package com.egm.stellio.apigateway

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.gateway.route.RouteLocator
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder
import org.springframework.context.annotation.Bean

@SpringBootApplication
class ApiGatewayApplication {
    @Value("\${application.entity-service.url:entity-service}")
    private val entityServiceUrl: String = ""

    @Value("\${application.search-service.url:search-service}")
    private val searchServiceUrl: String = ""

    @Value("\${application.subscription-service.url:subscription-service}")
    private val subscriptionServiceUrl: String = ""

    @Bean
    fun myRoutes(builder: RouteLocatorBuilder): RouteLocator {
        return builder.routes()
            .route { p ->
                p.path(
                    "/ngsi-ld/v1/entities/**",
                    "/ngsi-ld/v1/entityOperations/**",
                    "/ngsi-ld/v1/entityAccessControl/**",
                    "/ngsi-ld/v1/types/**",
                    "/entity/admin/**"
                )
                    .filters {
                        it.tokenRelay()
                    }
                    .uri("http://$entityServiceUrl:8082")
            }
            .route { p ->
                p.path(
                    "/ngsi-ld/v1/temporal/entities/**",
                    "/ngsi-ld/v1/temporal/entityOperations/**"
                )
                    .filters {
                        it.tokenRelay()
                    }
                    .uri("http://$searchServiceUrl:8083")
            }
            .route { p ->
                p.path("/ngsi-ld/v1/subscriptions/**")
                    .filters {
                        it.tokenRelay()
                    }
                    .uri("http://$subscriptionServiceUrl:8084")
            }
            .build()
    }
}

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    runApplication<ApiGatewayApplication>(*args)
}
