package com.egm.stellio.subscription.config

import org.flywaydb.core.Flyway
import org.springframework.context.annotation.Import

@Import(TestContainersConfiguration::class)
open class TimescaleBasedTests {

    init {
        val testContainers = TestContainersConfiguration.SubscriptionServiceTestContainers
        testContainers.startContainers()
        Flyway.configure()
            .dataSource(testContainers.getPostgresqlUri(), "stellio_subscription", "stellio_subscription_db_password")
            .load()
            .migrate()
    }
}
