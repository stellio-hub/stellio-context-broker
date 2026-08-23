package com.egm.stellio.search.common.tenant

import com.egm.stellio.shared.config.ApplicationProperties
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import jakarta.annotation.PostConstruct
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.r2dbc.autoconfigure.R2dbcProperties
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

@Configuration
@EnableTransactionManagement
class DatabaseTenantConfig(
    private val r2dbcProperties: R2dbcProperties,
    private val applicationProperties: ApplicationProperties,
    private val meterRegistry: MeterRegistry
) : AbstractR2dbcConfiguration() {

    internal val tenantConnectionFactories = mutableMapOf<String, ConnectionFactory>()

    @Bean
    fun transactionManager(connectionFactory: ConnectionFactory): ReactiveTransactionManager =
        R2dbcTransactionManager(connectionFactory)

    @Bean("connectionFactory")
    @Qualifier("connectionFactory")
    override fun connectionFactory(): ConnectionFactory {
        val connectionFactory = DatabaseTenantConnectionFactory(applicationProperties)
        val defaultFactory = defaultConnectionFactory()
        connectionFactory.setDefaultTargetConnectionFactory(defaultFactory)
        connectionFactory.setTargetConnectionFactories(tenantConnectionFactories)
        connectionFactory.setLenientFallback(false)
        // bindPoolMetrics("default", defaultFactory)
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
                .from(ConnectionFactoryOptions.parse(r2dbcProperties.url as String))
                .option(ConnectionFactoryOptions.USER, r2dbcProperties.username as String)
                .option(ConnectionFactoryOptions.PASSWORD, r2dbcProperties.password as String)
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
            createTenantConnectionFactory(tenantConfiguration.name, tenantConfiguration.dbSchema)
        }
    }

    fun createTenantConnectionFactory(name: String, dbSchema: String) {
        val tenantConnectionFactory = ConnectionFactories.get(
            ConnectionFactoryOptions.builder()
                .let {
                    if (r2dbcProperties.url?.contains("?") == true)
                        it.from(ConnectionFactoryOptions.parse(r2dbcProperties.url + "&schema=" + dbSchema))
                    else
                        it.from(ConnectionFactoryOptions.parse(r2dbcProperties.url + "?schema=" + dbSchema))
                }
                .option(ConnectionFactoryOptions.USER, r2dbcProperties.username as String)
                .option(ConnectionFactoryOptions.PASSWORD, r2dbcProperties.password as String)
                .build()
        )
        tenantConnectionFactories.putIfAbsent(name, tenantConnectionFactory)
        // bindPoolMetrics(name, tenantConnectionFactory)
    }

    // Exposes r2dbc-pool's own PoolMetrics (acquired/allocated/idle/pending connection counts) as
    // Micrometer gauges, tagged by tenant, so pool exhaustion (pendingAcquireSize > 0, acquiredSize
    // pinned at maxAllocatedSize) can be told apart from genuine query slowness under load.
    // Call sites are toggled on/off per perf-test run - suppressed rather than removed.
    @Suppress("UnusedPrivateMember")
    private fun bindPoolMetrics(tenant: String, connectionFactory: ConnectionFactory) {
        val pool = connectionFactory as? ConnectionPool ?: return
        val metrics = pool.metrics.orElse(null) ?: return

        Gauge.builder("r2dbc.pool.acquired", metrics) { it.acquiredSize().toDouble() }
            .description("Connections currently acquired and in active use")
            .tag("tenant", tenant)
            .register(meterRegistry)
        Gauge.builder("r2dbc.pool.allocated", metrics) { it.allocatedSize().toDouble() }
            .description("Connections currently allocated (acquired or idle)")
            .tag("tenant", tenant)
            .register(meterRegistry)
        Gauge.builder("r2dbc.pool.idle", metrics) { it.idleSize().toDouble() }
            .description("Connections currently idle in the pool")
            .tag("tenant", tenant)
            .register(meterRegistry)
        Gauge.builder("r2dbc.pool.pending", metrics) { it.pendingAcquireSize().toDouble() }
            .description("Acquire requests currently waiting for a connection - the key pool-exhaustion signal")
            .tag("tenant", tenant)
            .register(meterRegistry)
        Gauge.builder("r2dbc.pool.max", metrics) { it.maxAllocatedSize.toDouble() }
            .description("Configured maximum pool size")
            .tag("tenant", tenant)
            .register(meterRegistry)
    }
}
