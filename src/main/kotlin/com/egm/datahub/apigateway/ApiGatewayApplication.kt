package com.egm.datahub.apigateway

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder
import org.springframework.cloud.gateway.route.RouteLocator
import org.springframework.context.annotation.Bean

@SpringBootApplication
class ApiGatewayApplication {

	@Bean
	fun myRoutes(builder: RouteLocatorBuilder): RouteLocator {
		return builder.routes()
				.route { p ->
					p.path("/ngsi-ld/v1/entities")
							// TODO : configurable version
							.uri("http://context-registry:8081")
				}
				.build()
	}
}

fun main(args: Array<String>) {
	runApplication<ApiGatewayApplication>(*args)
}
