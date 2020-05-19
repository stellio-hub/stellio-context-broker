package com.egm.stellio.search.service

import TestContainersConfiguration
import com.egm.stellio.search.model.*
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.springframework.boot.test.context.SpringBootTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Import
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier
import java.time.OffsetDateTime
import java.util.*
import kotlin.random.Random

@SpringBootTest
@ActiveProfiles("test")
@Import(TestContainersConfiguration::class)
class AttributeInstanceServiceTests {

    @Autowired
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Autowired
    private lateinit var databaseClient: DatabaseClient

    private val observationDateTime = OffsetDateTime.now()

    private lateinit var temporalEntityAttribute: TemporalEntityAttribute

    init {
        val testContainersConfiguration = TestContainersConfiguration.TestContainers
        testContainersConfiguration.startContainers()
        Flyway.configure()
            .dataSource(testContainersConfiguration.getPostgresqlUri(), "stellio_search", "stellio_search_db_password")
            .load()
            .migrate()
    }

    @BeforeAll
    fun createTemporalEntityAttribute() {
        temporalEntityAttribute = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:BeeHive:TESTC",
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )

        databaseClient.insert()
            .into(TemporalEntityAttribute::class.java)
            .using(temporalEntityAttribute)
            .then()
            .block()
    }

    @AfterEach
    fun clearPreviousObservations() {
        databaseClient.delete()
            .from("attribute_instance")
            .fetch()
            .rowsUpdated()
            .block()
    }

    @Test
    fun `it should retrieve an observation and return the filled entity`() {

        val observation = gimmeAttributeInstance().copy(
            observedAt = observationDateTime,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation).block()

        val temporalQuery = TemporalQuery(emptyList(), TemporalQuery.Timerel.AFTER, OffsetDateTime.now().minusHours(1), null, null, null)
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 1 &&
                    it[0]["attribute_name"] == "incoming" &&
                    it[0]["value"] == 12.4 &&
                    it[0]["observed_at"] == observationDateTime &&
                    (it[0]["instance_id"] as String).startsWith("urn:ngsi-ld:Instance:")
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve all known observations and return the filled entity`() {

        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()).block() }

        val temporalQuery = TemporalQuery(emptyList(), TemporalQuery.Timerel.AFTER, OffsetDateTime.now().minusHours(1),
            null, null, null)
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 10
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should aggregate all observations for a day and return the filled entity`() {

        (1..10).forEach { _ ->
            val attributeInstance = gimmeAttributeInstance().copy(measuredValue = 1.0)
            attributeInstanceService.create(attributeInstance).block()
        }

        val temporalQuery = TemporalQuery(emptyList(), TemporalQuery.Timerel.AFTER, OffsetDateTime.now().minusHours(1),
            null, "1 day", TemporalQuery.Aggregate.SUM)
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 1 &&
                    it[0]["value"] == 10.0
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should only retrieve the temporal evolution of the provided temporal entity atttribute`() {

        val temporalEntityAttribute2 = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:BeeHive:TESTC",
            type = "BeeHive",
            attributeName = "outgoing",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )

        databaseClient.insert()
            .into(TemporalEntityAttribute::class.java)
            .using(temporalEntityAttribute2)
            .then()
            .block()

        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()).block() }
        (1..5).forEach { _ ->
            attributeInstanceService.create(
                gimmeAttributeInstance().copy(temporalEntityAttribute = temporalEntityAttribute2.id)).block()
        }

        val temporalQuery = TemporalQuery(emptyList(), TemporalQuery.Timerel.AFTER, OffsetDateTime.now().minusHours(1),
            null, null, null)
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 10
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should not retrieve temporal data if temporal entity does not match`() {

        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()).block() }

        val temporalQuery = TemporalQuery(emptyList(), TemporalQuery.Timerel.AFTER, OffsetDateTime.now().minusHours(1),
            null, null, null)
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute.copy(id = UUID.randomUUID()))

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.isEmpty()
            }
            .expectComplete()
            .verify()
    }

    private fun gimmeAttributeInstance(): AttributeInstance {
        return AttributeInstance(
            temporalEntityAttribute = temporalEntityAttribute.id,
            measuredValue = Random.nextDouble(),
            observedAt = OffsetDateTime.now()
        )
    }
}