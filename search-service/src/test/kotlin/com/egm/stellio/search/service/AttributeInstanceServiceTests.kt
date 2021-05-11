package com.egm.stellio.search.service

import com.egm.stellio.search.config.TimescaleBasedTests
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.FullAttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils.EGM_OBSERVED_BY
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import io.mockk.*
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

    val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @BeforeAll
    fun createTemporalEntityAttribute() {
        temporalEntityAttribute = TemporalEntityAttribute(
            entityId = entityId,
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
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it as List<FullAttributeInstanceResult>
                it.size == 1
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
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 10
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve all known observations and return the filled entity without timerel and time parameters`() {
        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()).block() }

        val enrichedEntity = attributeInstanceService.search(TemporalQuery(), temporalEntityAttribute, false)

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
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 1
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
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

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
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

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
            entityId = entityId,
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
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

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
            attributeInstanceService.search(temporalQuery, temporalEntityAttribute.copy(id = UUID.randomUUID()), false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.isEmpty()
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should not allow to create two attribute instances with same observation date`() {
        val attributeInstance = gimmeAttributeInstance()

        attributeInstanceService.create(attributeInstance).block()

        StepVerifier.create(attributeInstanceService.create(attributeInstance))
            .expectNextMatches {
                it == -1
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should create an AttributeInstance if it has a non null value or measuredValue`() {
        val attributeInstanceService = spyk(AttributeInstanceService(databaseClient), recordPrivateCalls = true)
        val observationPayload =
            """
        {
          "outgoing": {
            "type": "Property",
            "value": 550.0
          }
        }
            """.trimIndent()
        val parsedObservationPayload = JsonUtils.deserializeObject(observationPayload)
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

        attributeInstanceService.addAttributeInstances(
            temporalEntityAttribute.id,
            "outgoing",
            attributeValues,
            parsedObservationPayload
        ).block()

        verify {
            attributeInstanceService["create"](
                match<AttributeInstance> {
                    it.measuredValue == 550.0 &&
                        it.payload.matchContent(
                            """{"type": "Property","value": 550.0, "instanceId": "${it.instanceId}"}"""
                        )
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should not create an AttributeInstance if it has a null value and null measuredValue`() {
        val observationPayload =
            """
        {
          "outgoing": {
            "type": "Property",
            "observedBy": "2015-10-18T11:20:30.000001Z"
          }
        }
            """.trimIndent()
        val parsedObservationPayload = JsonUtils.deserializeObject(observationPayload)

        val attributeValues = mapOf(
            EGM_OBSERVED_BY to listOf(
                mapOf(
                    "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                )
            )
        )

        assertThrows<BadRequestDataException>("Value cannot be null") {
            attributeInstanceService.addAttributeInstances(
                temporalEntityAttribute.id,
                "outgoing",
                attributeValues,
                parsedObservationPayload
            )
        }
    }

    @Test
    fun `it should delete all temporal attribute instances of an entity`() {
        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()).block() }

        val deletedRecords = attributeInstanceService.deleteAttributeInstancesOfEntity(entityId).block()

        assert(deletedRecords == 10)
    }

    @Test
    fun `it should delete all temporal attribute instances of a temporal attribute`() {
        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()).block() }

        val deletedRecords = attributeInstanceService.deleteAttributeInstancesOfTemporalAttribute(
            entityId,
            "incoming",
            null
        ).block()

        assert(deletedRecords == 10)
    }

    private fun gimmeAttributeInstance(): AttributeInstance {
        val measuredValue = Random.nextDouble()
        val observedAt = Instant.now().atZone(ZoneOffset.UTC)
        return AttributeInstance(
            temporalEntityAttribute = temporalEntityAttribute.id,
            measuredValue = measuredValue,
            observedAt = observedAt,
            payload = mapOf(
                "type" to "Property",
                "value" to measuredValue,
                "observedAt" to observedAt
            )
        )
    }
}
