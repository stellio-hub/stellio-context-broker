package com.egm.datahub.context.search.service

import com.egm.datahub.context.search.model.*
import com.egm.datahub.context.search.service.MyPostgresqlContainer.DB_PASSWORD
import com.egm.datahub.context.search.service.MyPostgresqlContainer.DB_USER
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import org.flywaydb.core.Flyway
import org.springframework.boot.test.context.SpringBootTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.containers.PostgreSQLContainer
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.OffsetDateTime

@SpringBootTest
@ActiveProfiles("test")
@Import(R2DBCConfiguration::class)
class ObservationServiceTests {

    @MockkBean
    private lateinit var contextRegistryService: ContextRegistryService

    @Autowired
    private lateinit var observationService: ObservationService

    private val sensorId = "urn:ngsi-ld:Sensor:111222"

    private val observationDateTime = OffsetDateTime.now()

    init {
        Flyway.configure()
            .dataSource(MyPostgresqlContainer.instance.jdbcUrl, DB_USER, DB_PASSWORD)
            .load()
            .migrate()
    }

    @Test
    fun `it should retrieve an observation and return the filled entity`() {

        val entity = Entity()
        entity.addProperty("type", "Sensor")
        entity.addProperty("id", "urn:sosa:Sensor:1234")
        entity.addProperty("measures", mapOf("type" to "Property", "unitCode" to "CEL"))

        observationService.create(gimmeObservation()).block()

        every { contextRegistryService.getEntityById(any(), any()) } returns Mono.just(entity)

        val temporalQuery = TemporalQuery(TemporalQuery.Timerel.AFTER, OffsetDateTime.now().minusHours(1),
            null, sensorId)
        val enrichedEntity = observationService.search(temporalQuery, "Bearer 1234")

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                val values = (it.getPropertyValue("measures") as Map<*, *>)["values"]
                it.getPropertyValue("type") is String &&
                (it.getPropertyValue("type") as String) == "Sensor" &&
                it.getPropertyValue("measures") is Map<*, *> &&
                values is List<*> &&
                (values[0] as List<*>)[0] == 12.4 &&
                (values[0] as List<*>)[1] == observationDateTime
            }
            .expectComplete()
            .verify()

        verify { contextRegistryService.getEntityById(eq(sensorId), any()) }
        confirmVerified(contextRegistryService)
    }

    private fun gimmeObservation(): Observation {
        return Observation(
            attributeName = "incoming",
            latitude = 43.12,
            longitude = 65.43,
            observedBy = sensorId,
            unitCode = "CEL",
            value = 12.4,
            observedAt = observationDateTime
        )
    }
}

class KPostgreSQLContainer(imageName: String) : PostgreSQLContainer<KPostgreSQLContainer>(imageName)

object MyPostgresqlContainer {

    const val DB_NAME = "context_search_test"
    const val DB_USER = "datahub"
    const val DB_PASSWORD = "password"
    // TODO later extract it to a props file or load from env variable
    private const val TIMESCALE_IMAGE = "timescale/timescaledb-postgis:latest-pg11"

    val instance by lazy { startPostgresqlContainer() }

    private fun startPostgresqlContainer() = KPostgreSQLContainer(TIMESCALE_IMAGE).apply {
        withDatabaseName(DB_NAME)
        withUsername(DB_USER)
        withPassword(DB_PASSWORD)

        start()
    }
}

@TestConfiguration
class R2DBCConfiguration {

    @Bean
    fun connectionFactory(): ConnectionFactory {
        val options = ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DATABASE, MyPostgresqlContainer.instance.databaseName)
            .option(ConnectionFactoryOptions.HOST, MyPostgresqlContainer.instance.containerIpAddress)
            .option(ConnectionFactoryOptions.PORT, MyPostgresqlContainer.instance.firstMappedPort)
            .option(ConnectionFactoryOptions.USER, DB_USER)
            .option(ConnectionFactoryOptions.PASSWORD, DB_PASSWORD)
            .option(ConnectionFactoryOptions.DRIVER, "postgresql")
            .build()

        return ConnectionFactories.get(options)
    }
}
