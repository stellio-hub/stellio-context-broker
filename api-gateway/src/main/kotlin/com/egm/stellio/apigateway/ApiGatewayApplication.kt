package com.egm.stellio.apigateway

import com.egm.stellio.apigateway.filter.DuplicateRequestFilter
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.gateway.route.RouteLocator
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder
import org.springframework.context.annotation.Bean

@SpringBootApplication
class ApiGatewayApplication {

    @Value($$"${application.search-service.url:http://search-service:8083}")
    private val searchServiceUrl: String = ""

    @Value($$"${application.subscription-service.url:http://subscription-service:8084}")
    private val subscriptionServiceUrl: String = ""

    @Bean
    fun myRoutes(builder: RouteLocatorBuilder): RouteLocator =
        builder.routes()
            .route { p ->
                p.path(
                    "/ngsi-ld/v1/auth/**",
                    "/ngsi-ld/v1/entities/**",
                    "/ngsi-ld/v1/entityOperations/**",
                    "/ngsi-ld/v1/entityAccessControl/**",
                    "/ngsi-ld/v1/types/**",
                    "/ngsi-ld/v1/attributes/**",
                    "/ngsi-ld/v1/temporal/entities/**",
                    "/ngsi-ld/v1/temporal/entityOperations/**",
                    "/ngsi-ld/v1/csourceRegistrations/**"
                ).uri(searchServiceUrl)
            }
            .route { p ->
                p.path(
                    "/ngsi-ld/v1/subscriptions/**"
                ).uri(subscriptionServiceUrl)
            }
            .route { p ->
                p.path(
                    "/ngsi-ld/v1/jsonldContexts/**"
                ).filters { f ->
                    f.filter(
                        DuplicateRequestFilter(subscriptionServiceUrl)
                    )
                }.uri(searchServiceUrl)
            }
            .route("search_service_actuator") { p ->
                p.path("/search-service/actuator/**")
                    .filters { f ->
                        f.rewritePath(
                            "/search-service/actuator/(?<segment>/?.*)",
                            "/actuator/\${segment}"
                        )
                    }
                    .uri(searchServiceUrl)
            }
            .route("subscription_service_actuator") { p ->
                p.path("/subscription-service/actuator/**")
                    .filters { f ->
                        f.rewritePath(
                            "/subscription-service/actuator/(?<segment>/?.*)",
                            "/actuator/\${segment}"
                        )
                    }
                    .uri(subscriptionServiceUrl)
            }
            .build()
}

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    runApplication<ApiGatewayApplication>(*args)
}
