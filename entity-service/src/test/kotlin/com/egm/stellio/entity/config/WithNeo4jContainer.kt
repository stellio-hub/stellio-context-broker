package com.egm.stellio.entity.config

import ac.simons.neo4j.migrations.springframework.boot.autoconfigure.MigrationsAutoConfiguration
import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.Neo4jContainer

@ImportAutoConfiguration(MigrationsAutoConfiguration::class)
interface WithNeo4jContainer {

    companion object {

        private val neo4jContainer = Neo4jContainer<Nothing>("neo4j:4.4").apply {
            withNeo4jConfig("dbms.default_database", "stellio")
            withEnv("NEO4JLABS_PLUGINS", "[\"apoc\"]")
            withAdminPassword("neo4j_password")
            withReuse(true)
        }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.neo4j.uri") { neo4jContainer.boltUrl }
            registry.add("spring.neo4j.authentication.username") { "neo4j" }
            registry.add("spring.neo4j.authentication.password") { neo4jContainer.adminPassword }
        }

        init {
            neo4jContainer.start()
        }
    }
}
