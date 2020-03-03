package com.egm.datahub.context.subscription.service

import com.egm.datahub.context.subscription.config.MyPostgresqlContainer
import com.egm.datahub.context.subscription.config.R2DBCConfiguration
import org.flywaydb.core.Flyway
import org.springframework.context.annotation.Import

@Import(R2DBCConfiguration::class)
open class TimescaleBasedTests {

    init {
        Flyway.configure()
            .dataSource(MyPostgresqlContainer.instance.jdbcUrl, MyPostgresqlContainer.DB_USER, MyPostgresqlContainer.DB_PASSWORD)
            .load()
            .migrate()
    }
}