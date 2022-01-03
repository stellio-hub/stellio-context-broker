package com.egm.stellio.search.support

import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

interface WithTimescaleContainer {

    companion object {

        private const val DB_NAME = "stellio_search"
        private const val DB_USER = "stellio_search"
        private const val DB_PASSWORD = "stellio_search_db_password"

        private val timescaleImage: DockerImageName =
            DockerImageName.parse("stellio/stellio-timescale-postgis:2.3.0-pg13")
                .asCompatibleSubstituteFor("postgres")

        private val timescaleContainer = PostgreSQLContainer<Nothing>(timescaleImage).apply {
            withEnv("POSTGRES_PASSWORD", "password")
            withEnv("POSTGRES_MULTIPLE_DATABASES", "$DB_NAME,$DB_USER,$DB_PASSWORD")
            withReuse(true)
        }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            val containerAddress = "${timescaleContainer.containerIpAddress}:${timescaleContainer.firstMappedPort}"
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
