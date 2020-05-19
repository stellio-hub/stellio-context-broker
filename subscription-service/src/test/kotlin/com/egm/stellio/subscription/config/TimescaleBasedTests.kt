package com.egm.stellio.subscription.config

import org.flywaydb.core.Flyway
import org.springframework.context.annotation.Import

@Import(TestContainersConfiguration::class)
open class TimescaleBasedTests {

    init {
        val testContainersConfiguration = TestContainersConfiguration.TestContainers
        testContainersConfiguration.startContainers()
        Flyway.configure()
            .dataSource(testContainersConfiguration.getPostgresqlUri(), "stellio_subscription", "stellio_subscription_db_password")
            .load()
            .migrate()
    }
}