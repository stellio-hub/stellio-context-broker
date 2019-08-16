package com.egm.datahub.context.registry.config

import com.egm.datahub.context.registry.config.properties.Neo4jProperties

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.sql.Connection
import java.sql.DriverManager


@Configuration
class Neo4jConfig(
        private val neo4jProperties: Neo4jProperties
) {
    @Bean
    fun getNeo4jConnection(): Connection =
        DriverManager.getConnection("jdbc:neo4j:bolt://${neo4jProperties.url}", neo4jProperties.username, neo4jProperties.password)
}
