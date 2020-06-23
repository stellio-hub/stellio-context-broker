package com.egm.stellio.apigateway

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.gateway.route.RouteLocator
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder
import org.springframework.cloud.security.oauth2.gateway.TokenRelayGatewayFilterFactory
import org.springframework.context.annotation.Bean

@SpringBootApplication
class ApiGatewayApplication(
    private val filterFactory: TokenRelayGatewayFilterFactory
) {
    @Value("\${entity.service.url:entity-service}")
    private val entityServiceUrl: String = ""

    @Value("\${search.service.url:search-service}")
    private val searchServiceUrl: String = ""

    @Value("\${subscription.service.url:subscription-service}")
    private val subscriptionServiceUrl: String = ""

    @Bean
    fun myRoutes(builder: RouteLocatorBuilder): RouteLocator {
        return builder.routes()
            .route { p ->
                p.path("/ngsi-ld/v1/entities/**")
                    .filters {
                        it.filter(filterFactory.apply())
                    }
                    .uri("http://$entityServiceUrl:8082")
            }
            .route { p ->
                p.path("/ngsi-ld/v1/entityOperations/**")
                    .filters {
                        it.filter(filterFactory.apply())
                    }
                    .uri("http://$entityServiceUrl:8082")
            }
            .route { p ->
                p.path("/ngsi-ld/v1/temporal/entities/**")
                    .filters {
                        it.filter(filterFactory.apply())
                    }
                    .uri("http://$searchServiceUrl:8083")
            }
            .route { p ->
                p.path("/ngsi-ld/v1/subscriptions/**")
                    .filters {
                        it.filter(filterFactory.apply())
                    }
                    .uri("http://$subscriptionServiceUrl:8084")
            }
            .build()
    }
}

fun main(args: Array<String>) {
    runApplication<ApiGatewayApplication>(*args)
}
