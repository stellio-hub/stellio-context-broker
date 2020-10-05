package com.egm.stellio.search.service

import com.egm.stellio.search.config.TimescaleBasedTests
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils.EGM_OBSERVED_BY
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import kotlin.random.Random

@SpringBootTest
@ActiveProfiles("test")
class AttributeInstanceServiceTests : TimescaleBasedTests() {

    @Autowired
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Autowired
    private lateinit var databaseClient: DatabaseClient

    private val observationDateTime = Instant.now().atZone(ZoneOffset.UTC)

    private lateinit var temporalEntityAttribute: TemporalEntityAttribute

    @BeforeAll
    fun createTemporalEntityAttribute() {
        temporalEntityAttribute = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
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

        val temporalQuery = TemporalQuery(
            emptySet(),
            TemporalQuery.Timerel.AFTER,
            Instant.now().atZone(ZoneOffset.UTC).minusHours(1)
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 1 &&
                    it[0].attributeName == "incoming" &&
                    it[0].value == 12.4 &&
                    ZonedDateTime.parse(it[0].observedAt.format(DateTimeFormatter.ISO_DATE_TIME)).toInstant()
                    .atZone(ZoneOffset.UTC) == observationDateTime &&
                    (it[0].instanceId.toString()).startsWith("urn:ngsi-ld:Instance:")
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve all known observations and return the filled entity`() {
        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()).block() }

        val temporalQuery = TemporalQuery(
            emptySet(),
            TemporalQuery.Timerel.AFTER,
            Instant.now().atZone(ZoneOffset.UTC).minusHours(1)
        )
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

        val temporalQuery = TemporalQuery(
            emptySet(), TemporalQuery.Timerel.AFTER, Instant.now().atZone(ZoneOffset.UTC).minusHours(1),
            null, "1 day", TemporalQuery.Aggregate.SUM
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 1 &&
                    it[0].value == 10.0
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should only return the last n aggregates asked in the temporal query`() {
        (1..10).forEachIndexed { index, _ ->
            val attributeInstance =
                gimmeAttributeInstance()
                    .copy(
                        measuredValue = 1.0,
                        observedAt = Instant.now().atZone(ZoneOffset.UTC).minusHours(index.toLong())
                    )
            attributeInstanceService.create(attributeInstance).block()
        }

        val temporalQuery = TemporalQuery(
            emptySet(), TemporalQuery.Timerel.AFTER, Instant.now().atZone(ZoneOffset.UTC).minusHours(12),
            null, "2 hours", TemporalQuery.Aggregate.SUM, 3
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 3
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should only return the last n observations asked in the temporal query`() {
        (1..10).forEach { _ ->
            val attributeInstance = gimmeAttributeInstance().copy(measuredValue = 1.0)
            attributeInstanceService.create(attributeInstance).block()
        }

        val temporalQuery = TemporalQuery(
            emptySet(),
            TemporalQuery.Timerel.AFTER,
            Instant.now().atZone(ZoneOffset.UTC).minusHours(1),
            lastN = 5
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 5
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should only retrieve the temporal evolution of the provided temporal entity attribute`() {
        val temporalEntityAttribute2 = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
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
                gimmeAttributeInstance().copy(temporalEntityAttribute = temporalEntityAttribute2.id)
            ).block()
        }

        val temporalQuery = TemporalQuery(
            emptySet(),
            TemporalQuery.Timerel.AFTER,
            Instant.now().atZone(ZoneOffset.UTC).minusHours(1)
        )
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

        val temporalQuery = TemporalQuery(
            emptySet(),
            TemporalQuery.Timerel.AFTER,
            Instant.now().atZone(ZoneOffset.UTC).minusHours(1)
        )
        val enrichedEntity =
            attributeInstanceService.search(temporalQuery, temporalEntityAttribute.copy(id = UUID.randomUUID()))

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.isEmpty()
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should create an AttributeInstance if it has a non null value or measuredValue`() {
        val attributeValues = mapOf(
            EGM_OBSERVED_BY to listOf(
                mapOf(
                    "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                )
            ),
            NGSILD_PROPERTY_VALUE to listOf(
                mapOf(
                    JSONLD_VALUE_KW to 550.0
                )
            )
        )

        val attributeInstance =
            attributeInstanceService.addAttributeInstances(temporalEntityAttribute.id, "outgoing", attributeValues)

        StepVerifier.create(attributeInstance)
            .expectNextMatches {
                it > 0
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should not create an AttributeInstance if it has a null value and null measuredValue`() {
        val attributeValues = mapOf(
            EGM_OBSERVED_BY to listOf(
                mapOf(
                    "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                )
            )
        )

        assertThrows<BadRequestDataException>("Value cannot be null") {
            attributeInstanceService.addAttributeInstances(temporalEntityAttribute.id, "outgoing", attributeValues)
        }
    }

    private fun gimmeAttributeInstance(): AttributeInstance {
        return AttributeInstance(
            temporalEntityAttribute = temporalEntityAttribute.id,
            measuredValue = Random.nextDouble(),
            observedAt = Instant.now().atZone(ZoneOffset.UTC)
        )
    }
}
