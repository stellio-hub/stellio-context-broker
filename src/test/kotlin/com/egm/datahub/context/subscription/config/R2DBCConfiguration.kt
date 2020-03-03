package com.egm.datahub.context.subscription.config

import com.egm.datahub.context.subscription.config.MyPostgresqlContainer.DB_PASSWORD
import com.egm.datahub.context.subscription.config.MyPostgresqlContainer.DB_USER
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.testcontainers.containers.PostgreSQLContainer

class KPostgreSQLContainer(imageName: String) : PostgreSQLContainer<KPostgreSQLContainer>(imageName)

object MyPostgresqlContainer {

    const val DB_NAME = "context_subscription_test"
    const val DB_USER = "datahub"
    const val DB_PASSWORD = "password"
    // TODO later extract it to a props file or load from env variable
    private const val TIMESCALE_IMAGE = "timescale/timescaledb-postgis:latest-pg11"

    val instance by lazy { startPostgresqlContainer() }

    private fun startPostgresqlContainer() = KPostgreSQLContainer(TIMESCALE_IMAGE).apply {
        withDatabaseName(DB_NAME)
        withUsername(DB_USER)
        withPassword(DB_PASSWORD)

        start()
    }
}

@TestConfiguration
class R2DBCConfiguration {

    @Bean
    fun connectionFactory(): ConnectionFactory {
        val options = ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DATABASE, MyPostgresqlContainer.instance.databaseName)
            .option(ConnectionFactoryOptions.HOST, MyPostgresqlContainer.instance.containerIpAddress)
            .option(ConnectionFactoryOptions.PORT, MyPostgresqlContainer.instance.firstMappedPort)
            .option(ConnectionFactoryOptions.USER, DB_USER)
            .option(ConnectionFactoryOptions.PASSWORD, DB_PASSWORD)
            .option(ConnectionFactoryOptions.DRIVER, "postgresql")
            .build()

        return ConnectionFactories.get(options)
    }
}
