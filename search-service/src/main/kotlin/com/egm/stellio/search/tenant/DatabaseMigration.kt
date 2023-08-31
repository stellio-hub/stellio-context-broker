package com.egm.stellio.search.tenant

import com.egm.stellio.shared.config.ApplicationProperties
import jakarta.annotation.PostConstruct
import org.flywaydb.core.Flyway
import org.springframework.boot.autoconfigure.flyway.FlywayProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.jdbc.DataSourceBuilder
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.datasource.SimpleDriverDataSource
import javax.sql.DataSource

@Configuration
@EnableConfigurationProperties(FlywayProperties::class)
class DatabaseMigration(
    private val applicationProperties: ApplicationProperties,
    private val flywayProperties: FlywayProperties
) {

    @PostConstruct
    fun migrateFlyway() {
        applicationProperties.tenants.forEach { tenantConfiguration ->
            createFlyway(tenantConfiguration.dbSchema).migrate()
        }
    }

    fun createFlyway(schemaName: String): Flyway =
        Flyway(
            Flyway.configure()
                .baselineVersion(flywayProperties.baselineVersion)
                .baselineOnMigrate(flywayProperties.isBaselineOnMigrate)
                .defaultSchema(schemaName)
                .dataSource(flywayDataSource())
        )

    fun flywayDataSource(): DataSource =
        DataSourceBuilder.create()
            .driverClassName("org.postgresql.Driver")
            .type(SimpleDriverDataSource::class.java)
            .url(flywayProperties.url)
            .username(flywayProperties.user)
            .password(flywayProperties.password)
            .build()
}
