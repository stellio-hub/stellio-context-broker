package com.egm.datahub.context.registry.web

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.router

@Configuration
class Routes(
        private val entityHandler: EntityHandler,
        private val neo4jService: Neo4jService
) {

    @Bean
    fun router() = router {
        (accept(MediaType.valueOf("application/ld+json")) and "/ngsi-ld/v1")
                .nest {
                    "/entities/graphdb".nest {
                        POST("", entityHandler::create)
                        GET("{entityId}", entityHandler::getById)
                    }
                    "/entities/neo4j".nest {
                        POST("", neo4jService::create)
                        GET("/{label}", neo4jService::getEntitiesByLabel)
                    }
        }
    }
}
