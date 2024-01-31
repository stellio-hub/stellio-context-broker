package com.egm.stellio.subscription.support

import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

interface WithTimescaleContainer {

    companion object {

        private const val DB_NAME = "stellio_subscription"
        private const val DB_USER = "stellio_subscription"
        private const val DB_PASSWORD = "stellio_subscription_db_password"

        private val timescaleImage: DockerImageName =
            DockerImageName.parse("stellio/stellio-timescale-postgis:14-2.11.1-3.3")
                .asCompatibleSubstituteFor("postgres")

        private val timescaleContainer = GenericContainer<Nothing>(timescaleImage).apply {
            withEnv("POSTGRES_USER", DB_USER)
            withEnv("POSTGRES_PASS", DB_PASSWORD)
            withEnv("POSTGRES_DBNAME", DB_NAME)
            withEnv("POSTGRES_MULTIPLE_EXTENSIONS", "postgis,timescaledb,pgcrypto")
            withExposedPorts(5432)
            setWaitStrategy(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
        }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            val containerAddress = "${timescaleContainer.host}:${timescaleContainer.firstMappedPort}"
            registry.add("spring.r2dbc.url") { "r2dbc:postgresql://$containerAddress/$DB_NAME" }
            registry.add("spring.r2dbc.username") { DB_USER }
            registry.add("spring.r2dbc.password") { DB_PASSWORD }
            registry.add("spring.flyway.url") { "jdbc:postgresql://$containerAddress/$DB_NAME" }
            registry.add("spring.flyway.user") { DB_USER }
            registry.add("spring.flyway.password") { DB_PASSWORD }
        }

        init {
            timescaleContainer.start()
        }
    }
}
