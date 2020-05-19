package com.egm.stellio.search.config

import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.testcontainers.containers.DockerComposeContainer
import java.io.File

@TestConfiguration
class TestContainersConfiguration {

    class KDockerComposeContainer(file: File) : DockerComposeContainer<KDockerComposeContainer>(file)

    private val DB_NAME = "stellio_search"
    private val DB_USER = "stellio_search"
    private val DB_PASSWORD = "stellio_search_db_password"

    object TestContainers {

        private const val POSTGRESQL_SERVICE_NAME = "postgres"
        private val DOCKER_COMPOSE_FILE = File("docker-compose.yml")

        val instance: KDockerComposeContainer by lazy { defineDockerCompose() }

        private fun defineDockerCompose() =
            KDockerComposeContainer(
                DOCKER_COMPOSE_FILE
            ).withLocalCompose(true).withExposedService(POSTGRESQL_SERVICE_NAME, 5432)

        fun getPostgresqlHost(): String {
            return instance.getServiceHost(
                POSTGRESQL_SERVICE_NAME, 5432)
        }

        fun getPostgresqlPort(): Int {
            return instance.getServicePort(
                POSTGRESQL_SERVICE_NAME, 5432)
        }

        fun getPostgresqlUri(): String {
            return "jdbc:postgresql://" + getPostgresqlHost() + ":" + getPostgresqlPort() + '/'
        }

        fun startContainers() {
            instance.start()
        }
    }

    @Bean
    fun configuration(): ConnectionFactory {
        val options = ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DATABASE, DB_NAME)
            .option(ConnectionFactoryOptions.HOST,
                TestContainers.getPostgresqlHost()
            )
            .option(ConnectionFactoryOptions.PORT,
                TestContainers.getPostgresqlPort()
            )
            .option(ConnectionFactoryOptions.USER, DB_USER)
            .option(ConnectionFactoryOptions.PASSWORD, DB_PASSWORD)
            .option(ConnectionFactoryOptions.DRIVER, "postgresql")
            .build()

        return ConnectionFactories.get(options)
    }
}
