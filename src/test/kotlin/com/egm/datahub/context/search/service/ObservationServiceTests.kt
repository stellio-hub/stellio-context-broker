package com.egm.datahub.context.search.service

import com.egm.datahub.context.search.model.*
import com.egm.datahub.context.search.service.MyPostgresqlContainer.DB_PASSWORD
import com.egm.datahub.context.search.service.MyPostgresqlContainer.DB_USER
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
import reactor.test.StepVerifier
import java.time.OffsetDateTime
import kotlin.random.Random

@SpringBootTest
@ActiveProfiles("test")
@Import(R2DBCConfiguration::class)
class ObservationServiceTests {

    @Autowired
    private lateinit var observationService: ObservationService

    private val observationDateTime = OffsetDateTime.now()

    private val defaultSensorId = "urn:ngsi-ld:Sensor:111222"

    init {
        Flyway.configure()
            .dataSource(MyPostgresqlContainer.instance.jdbcUrl, DB_USER, DB_PASSWORD)
            .load()
            .migrate()
    }

    @Test
    fun `it should retrieve an observation and return the filled entity`() {

        val sensorId = "urn:ngsi-ld:Sensor:IncomingSensor"
        val observation = gimmeObservation().copy(
            observedAt = observationDateTime,
            value = 12.4,
            observedBy = sensorId
        )
        observationService.create(observation).block()

        val entityTemporalProperty = EntityTemporalProperty(
            entityId = "urn:ngsi-ld:BeeHive:TESTC",
            type = "BeeHive",
            attributeName = "incoming",
            observedBy = sensorId
        )
        val temporalQuery = TemporalQuery(TemporalQuery.Timerel.AFTER, OffsetDateTime.now().minusHours(1), null)
        val enrichedEntity = observationService.search(temporalQuery, entityTemporalProperty)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 1 &&
                    it[0]["VALUE"] == 12.4 &&
                    it[0]["OBSERVED_AT"] == observationDateTime
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should aggregate all known observations and return the filled entity`() {

        (1..10).forEach { _ -> observationService.create(gimmeObservation()).block() }

        val entityTemporalProperty = EntityTemporalProperty(
            entityId = "urn:ngsi-ld:BeeHive:TESTC",
            type = "BeeHive",
            attributeName = "incoming",
            observedBy = defaultSensorId
        )
        val temporalQuery = TemporalQuery(TemporalQuery.Timerel.AFTER, OffsetDateTime.now().minusHours(1),
            null)
        val enrichedEntity = observationService.search(temporalQuery, entityTemporalProperty)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 10
            }
            .expectComplete()
            .verify()
    }

    private fun gimmeObservation(): Observation {
        return Observation(
            attributeName = "incoming",
            latitude = 43.12,
            longitude = 65.43,
            observedBy = defaultSensorId,
            unitCode = "CEL",
            value = Random.nextDouble(),
            observedAt = OffsetDateTime.now()
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
