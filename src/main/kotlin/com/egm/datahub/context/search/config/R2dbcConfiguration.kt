package com.egm.datahub.context.search.config

import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions.*
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration
import org.springframework.transaction.annotation.EnableTransactionManagement

@Configuration
@EnableTransactionManagement
class R2dbcConfiguration(
        private val datasourceProperties: DatasourceProperties
) : AbstractR2dbcConfiguration() {

    @Bean
    override fun connectionFactory(): ConnectionFactory {
        // TODO this probably could be done with Spring Boot autoconfiguration
        val options = builder()
                .option(DATABASE, datasourceProperties.database)
                .option(HOST, datasourceProperties.host)
                .option(PORT, datasourceProperties.port)
                .option(USER, datasourceProperties.user)
                .option(PASSWORD, datasourceProperties.password)
                .option(DRIVER, datasourceProperties.driver)
                .build()
        return ConnectionFactories.get(options)
    }
}