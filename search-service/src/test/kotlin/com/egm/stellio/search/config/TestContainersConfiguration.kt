package com.egm.stellio.search.config

import com.egm.stellio.shared.TestContainers
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean

@TestConfiguration
class TestContainersConfiguration {

    private val DB_NAME = "stellio_search"
    private val DB_USER = "stellio_search"
    private val DB_PASSWORD = "stellio_search_db_password"

    object SearchServiceTestContainers : TestContainers("postgres", 5432) {

        fun getPostgresqlHost(): String {
            return instance.getServiceHost(serviceName, servicePort)
        }

        fun getPostgresqlPort(): Int {
            return instance.getServicePort(serviceName, servicePort)
        }

        fun getPostgresqlUri(): String {
            return "jdbc:postgresql://" + getPostgresqlHost() + ":" + getPostgresqlPort() + '/'
        }
    }

    @Bean
    fun connectionFactory(): ConnectionFactory {
        val options = ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DATABASE, DB_NAME)
            .option(ConnectionFactoryOptions.HOST, SearchServiceTestContainers.getPostgresqlHost())
            .option(ConnectionFactoryOptions.PORT, SearchServiceTestContainers.getPostgresqlPort())
            .option(ConnectionFactoryOptions.USER, DB_USER)
            .option(ConnectionFactoryOptions.PASSWORD, DB_PASSWORD)
            .option(ConnectionFactoryOptions.DRIVER, "postgresql")
            .build()

        return ConnectionFactories.get(options)
    }
}
