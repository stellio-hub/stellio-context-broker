package com.egm.stellio.entity.config

import org.springframework.boot.autoconfigure.data.neo4j.Neo4jProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.data.neo4j.repository.config.EnableNeo4jRepositories

@Profile("!test")
@Configuration
@EnableNeo4jRepositories(basePackages = ["com.egm.stellio.entity.repository"])
class Neo4jConfiguration {

    @Bean
    fun ogmConfiguration(properties: Neo4jProperties): org.neo4j.ogm.config.Configuration {
        return org.neo4j.ogm.config.Configuration.Builder()
            .uri(properties.uri)
            .credentials(properties.username, properties.password)
            .useNativeTypes()
            .database("stellio")
            .verifyConnection(true)
            .build()
    }
}
