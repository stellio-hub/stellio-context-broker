package com.egm.stellio.search.config

import org.flywaydb.core.Flyway
import org.springframework.context.annotation.Import

@Import(TestContainersConfiguration::class)
open class TimescaleBasedTests {

    init {
        val testContainersConfiguration = TestContainersConfiguration.TestContainers
        testContainersConfiguration.startContainers()
        Flyway.configure()
            .dataSource(testContainersConfiguration.getPostgresqlUri(), "stellio_search", "stellio_search_db_password")
            .load()
            .migrate()
    }
}