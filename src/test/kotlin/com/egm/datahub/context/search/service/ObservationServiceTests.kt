package com.egm.datahub.context.search.service

import com.egm.datahub.context.search.model.Entity
import com.egm.datahub.context.search.model.TemporalQuery
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import io.r2dbc.spi.ConnectionFactory
import org.junit.jupiter.api.BeforeAll
import org.springframework.boot.test.context.SpringBootTest
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.OffsetDateTime

@SpringBootTest
@EmbeddedKafka(topics = ["entities"])
@ActiveProfiles("test")
class ObservationServiceTests {

    @Autowired
    private lateinit var cf: ConnectionFactory

    @MockkBean
    private lateinit var contextRegistryService: ContextRegistryService

    @Autowired
    private lateinit var observationService: ObservationService

    private val sensorId = "urn:sosa:Sensor:111222"

    companion object {
        init {
            System.setProperty(EmbeddedKafkaBroker.BROKER_LIST_PROPERTY, "spring.kafka.bootstrap-servers")
        }
    }

    private lateinit var observationDateTime: OffsetDateTime

    @BeforeAll
    fun initDatabase() {

        observationDateTime = OffsetDateTime.now()

        val createObservationTable = """
            CREATE TABLE observation (
              observed_by   VARCHAR                   NOT NULL,
              observed_at   TIMESTAMP WITH TIME ZONE  NOT NULL,
              value         DOUBLE PRECISION          NOT NULL,
              unit_code     VARCHAR                   NOT NULL,
              latitude      DOUBLE PRECISION                  ,
              longitude     DOUBLE PRECISION                  ,
              UNIQUE (observed_by, observed_at, value, unit_code)
            );
        """.trimIndent()

        val insertObservationRow = """
            insert into observation (observed_by, observed_at, value, unit_code, latitude, longitude)
            values ('$sensorId', '$observationDateTime', 12.0, 'CEL', 43.12, 65.43)
        """.trimIndent()

        Flux.from(cf.create())
            .flatMap { c ->
                c.createBatch()
                    .add("drop table if exists observation")
                    .add(createObservationTable)
                    .add(insertObservationRow)
                    .execute()
            }
            .log()
            .blockLast()
    }

    @Test
    fun `it should retrieve an observation and return the filled entity`() {

        val entity = Entity()
        entity.addProperty("type", "Sensor")
        entity.addProperty("id", "urn:sosa:Sensor:1234")
        entity.addProperty("measures", mapOf("type" to "Property", "unitCode" to "CEL"))

        every { contextRegistryService.getEntityById(any()) } returns Mono.just(entity)

        val temporalQuery = TemporalQuery(TemporalQuery.Timerel.AFTER, OffsetDateTime.now().minusHours(1),
            null, sensorId)
        val enrichedEntity = observationService.search(temporalQuery)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                val values = (it.getPropertyValue("measures") as Map<*, *>)["values"]

                it.getPropertyValue("type") is String &&
                    (it.getPropertyValue("type") as String) == "Sensor" &&
                    it.getPropertyValue("measures") is Map<*, *> &&
                    values is List<*> &&
                    (values[0] as List<*>)[0] == 12.0 &&
                    (values[0] as List<*>)[1] == observationDateTime
            }
            .expectComplete()
            .verify()

        verify { contextRegistryService.getEntityById(eq(sensorId)) }
        confirmVerified(contextRegistryService)
    }
}
