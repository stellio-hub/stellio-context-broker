package com.egm.stellio.search.tenant

import com.egm.stellio.shared.config.ApplicationProperties
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import jakarta.annotation.PostConstruct
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.r2dbc.R2dbcProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy
import org.springframework.data.r2dbc.core.R2dbcEntityOperations
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.dialect.DialectResolver
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.annotation.EnableTransactionManagement
import java.net.URI

@Configuration
@EnableTransactionManagement
class DatabaseTenantConfig(
    private val r2dbcProperties: R2dbcProperties,
    private val applicationProperties: ApplicationProperties
) : AbstractR2dbcConfiguration() {

    internal val tenantConnectionFactories = mutableMapOf<String, ConnectionFactory>()

    @Bean
    fun transactionManager(connectionFactory: ConnectionFactory): ReactiveTransactionManager =
        R2dbcTransactionManager(connectionFactory)

    @Bean("connectionFactory")
    @Qualifier("connectionFactory")
    override fun connectionFactory(): ConnectionFactory {
        val connectionFactory = DatabaseTenantConnectionFactory(applicationProperties)
        connectionFactory.setDefaultTargetConnectionFactory(defaultConnectionFactory())
        connectionFactory.setTargetConnectionFactories(tenantConnectionFactories)
        connectionFactory.setLenientFallback(false)
        return connectionFactory
    }

    @Bean
    fun tenantEntityTemplate(
        @Qualifier("connectionFactory") connectionFactory: ConnectionFactory
    ): R2dbcEntityOperations =
        createEntityTemplate(connectionFactory)

    fun defaultConnectionFactory(): ConnectionFactory {
        return ConnectionFactories.get(
            ConnectionFactoryOptions.builder()
                .from(ConnectionFactoryOptions.parse(r2dbcProperties.url))
                .option(ConnectionFactoryOptions.USER, r2dbcProperties.username)
                .option(ConnectionFactoryOptions.PASSWORD, r2dbcProperties.password)
                .build()
        )
    }

    private fun createEntityTemplate(connectionFactory: ConnectionFactory): R2dbcEntityOperations {
        val dialect = DialectResolver.getDialect(connectionFactory)
        val strategy = DefaultReactiveDataAccessStrategy(dialect)
        val databaseClient = DatabaseClient.builder()
            .connectionFactory(connectionFactory)
            .bindMarkers(dialect.bindMarkersFactory)
            .build()
        return R2dbcEntityTemplate(databaseClient, strategy)
    }

    @PostConstruct
    fun initializeTenantDataSources() {
        applicationProperties.tenants.forEach { tenantConfiguration ->
            createTenantConnectionFactory(tenantConfiguration.uri, tenantConfiguration.dbSchema)
        }
    }

    fun createTenantConnectionFactory(uri: URI, dbSchema: String) {
        val tenantConnectionFactory = ConnectionFactories.get(
            ConnectionFactoryOptions.builder()
                .from(ConnectionFactoryOptions.parse(r2dbcProperties.url + "?schema=" + dbSchema))
                .option(ConnectionFactoryOptions.USER, r2dbcProperties.username)
                .option(ConnectionFactoryOptions.PASSWORD, r2dbcProperties.password)
                .build()
        )
        tenantConnectionFactories.putIfAbsent(uri.toString(), tenantConnectionFactory)
    }
}
