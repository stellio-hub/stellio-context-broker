package com.egm.stellio.entity.config

import com.egm.stellio.shared.TestContainers
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean

@TestConfiguration
class TestContainersConfiguration {

    private val DB_USER = "neo4j"
    private val DB_PASSWORD = "neo4j_password"

    object EntityServiceTestContainers : TestContainers("neo4j", 7687) {

        fun getNeo4jUri(): String {
            return "bolt://" + instance.getServiceHost(serviceName, servicePort) + ":" + instance.getServicePort(
                serviceName,
                servicePort
            )
        }
    }

    @Bean
    fun connectionFactory(): org.neo4j.ogm.config.Configuration {
        EntityServiceTestContainers.startContainers()

        return org.neo4j.ogm.config.Configuration.Builder()
            .uri(EntityServiceTestContainers.getNeo4jUri())
            .credentials(DB_USER, DB_PASSWORD)
            .useNativeTypes()
            .build()
    }
}
