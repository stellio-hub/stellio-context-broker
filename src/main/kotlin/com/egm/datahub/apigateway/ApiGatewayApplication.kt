package com.egm.datahub.apigateway

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder
import org.springframework.cloud.gateway.route.RouteLocator
import org.springframework.cloud.security.oauth2.gateway.TokenRelayGatewayFilterFactory
import org.springframework.context.annotation.Bean

@SpringBootApplication
class ApiGatewayApplication(
		private val filterFactory: TokenRelayGatewayFilterFactory
) {

	@Bean
	fun myRoutes(builder: RouteLocatorBuilder): RouteLocator {
		return builder.routes()
				.route { p ->
					p.path("/ngsi-ld/v1/entities")
							.filters {
								it.filter(filterFactory.apply())
							}
							// TODO : configurable version
							.uri("http://context-registry:8081")
				}
				.build()
	}
}

fun main(args: Array<String>) {
	runApplication<ApiGatewayApplication>(*args)
}
