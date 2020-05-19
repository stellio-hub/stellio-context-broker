package com.egm.stellio.entity.config

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.testcontainers.containers.DockerComposeContainer
import java.io.File

@TestConfiguration
class TestContainersConfiguration {

    private val DB_USER = "neo4j"
    private val DB_PASSWORD = "neo4j_password"

    class KDockerComposeContainer(file: File) : DockerComposeContainer<KDockerComposeContainer>(file)

    object TestContainers {

        private const val NEO4J_SERVICE_NAME = "neo4j"
        private val DOCKER_COMPOSE_FILE = File("docker-compose.yml")

        private val instance: KDockerComposeContainer by lazy { defineDockerCompose() }

        private fun defineDockerCompose() =
            KDockerComposeContainer(
                DOCKER_COMPOSE_FILE
            ).withLocalCompose(true).withExposedService(NEO4J_SERVICE_NAME, 7687)

        fun getNeo4jUri(): String {
            return "bolt://" + instance.getServiceHost(
                NEO4J_SERVICE_NAME, 7687) + ":" + instance.getServicePort(
                NEO4J_SERVICE_NAME, 7687)
        }

        fun startContainers() {
            instance.start()
        }
    }

    @Bean
    fun configuration(): org.neo4j.ogm.config.Configuration {
        TestContainers.startContainers()

        return org.neo4j.ogm.config.Configuration.Builder()
            .uri(TestContainers.getNeo4jUri())
            .credentials(DB_USER, DB_PASSWORD)
            .useNativeTypes()
            .build()
        }
}
