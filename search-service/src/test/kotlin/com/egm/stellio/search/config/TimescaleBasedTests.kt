package com.egm.stellio.search.config

import org.flywaydb.core.Flyway
import org.springframework.context.annotation.Import

@Import(TestContainersConfiguration::class)
open class TimescaleBasedTests {

    init {
        val testContainers = TestContainersConfiguration.SearchServiceTestContainers
        testContainers.startContainers()
        Flyway.configure()
            .dataSource(testContainers.getPostgresqlUri(), "stellio_search", "stellio_search_db_password")
            .load()
            .migrate()
    }
}
