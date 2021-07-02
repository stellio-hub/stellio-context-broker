package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.FullAttributeInstanceResult
import com.egm.stellio.search.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils.EGM_OBSERVED_BY
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import io.mockk.confirmVerified
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.insert
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import kotlin.random.Random

@SpringBootTest
@ActiveProfiles("test")
class AttributeInstanceServiceTests : WithTimescaleContainer {

    @Autowired
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Autowired
    private lateinit var databaseClient: DatabaseClient

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val now = Instant.now().atZone(ZoneOffset.UTC)

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

        r2dbcEntityTemplate.insert<TemporalEntityAttribute>()
            .using(temporalEntityAttribute)
            .then()
            .block()
    }

    @AfterEach
    fun clearPreviousObservations() {
        r2dbcEntityTemplate.delete(AttributeInstance::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should retrieve a full instance if temporalValues are not asked for`() {
        val observation = gimmeAttributeInstance().copy(
            observedAt = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation).block()

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            time = now.minusHours(1)
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 1 &&
                    it[0] is FullAttributeInstanceResult &&
                    it[0].temporalEntityAttribute == temporalEntityAttribute.id
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve all full instances if temporalValues are not asked for`() {
        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()).block() }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            time = now.minusHours(1)
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
    fun `it should retrieve all instances when no timerel and time parameters are provided`() {
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
    fun `it should retrieve instances of a temporal entity attribute whose value type is Any`() {
        val temporalEntityAttribute2 = TemporalEntityAttribute(
            entityId = entityId,
            type = "BeeHive",
            attributeName = "propWithStringValue",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
        )
        r2dbcEntityTemplate.insert<TemporalEntityAttribute>()
            .using(temporalEntityAttribute2)
            .then()
            .block()

        (1..10).forEach { _ ->
            val observedAt = Instant.now().atZone(ZoneOffset.UTC)
            val attributeInstance = AttributeInstance(
                temporalEntityAttribute = temporalEntityAttribute2.id,
                value = "some value",
                observedAt = observedAt,
                payload = mapOf(
                    "type" to "Property",
                    "value" to "some value",
                    "observedAt" to observedAt
                )
            )
            attributeInstanceService.create(attributeInstance).block()
        }

        val enrichedEntity = attributeInstanceService.search(TemporalQuery(), temporalEntityAttribute2, true)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 10 &&
                    it.all { attributeInstanceResult ->
                        attributeInstanceResult.temporalEntityAttribute == temporalEntityAttribute2.id &&
                            (attributeInstanceResult as SimplifiedAttributeInstanceResult).value == "some value"
                    }
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should sum all instances for a day`() {
        (1..10).forEach { _ ->
            val attributeInstance = gimmeAttributeInstance().copy(measuredValue = 1.0)
            attributeInstanceService.create(attributeInstance).block()
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            time = now.minusHours(1),
            timeBucket = "1 day",
            aggregate = TemporalQuery.Aggregate.SUM
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 1 &&
                    it[0] is SimplifiedAttributeInstanceResult &&
                    (it[0] as SimplifiedAttributeInstanceResult).value as Double == 10.0
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should count all instances for a day`() {
        (1..9).forEach { _ ->
            val attributeInstance = gimmeAttributeInstance().copy(measuredValue = 1.0)
            attributeInstanceService.create(attributeInstance).block()
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            time = now.minusHours(1),
            timeBucket = "1 day",
            aggregate = TemporalQuery.Aggregate.COUNT
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 1 &&
                    it[0] is SimplifiedAttributeInstanceResult &&
                    (it[0] as SimplifiedAttributeInstanceResult).value as Long == 9L
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
                        observedAt = now.minusHours(index.toLong())
                    )
            attributeInstanceService.create(attributeInstance).block()
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            time = now.minusHours(12),
            timeBucket = "2 hours",
            aggregate = TemporalQuery.Aggregate.SUM,
            lastN = 3
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
    fun `it should only return the last n instances asked in the temporal query`() {
        (1..10).forEach { _ ->
            val attributeInstance = gimmeAttributeInstance().copy(measuredValue = 1.0)
            attributeInstanceService.create(attributeInstance).block()
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            time = now.minusHours(1),
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

        r2dbcEntityTemplate.insert<TemporalEntityAttribute>()
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
            timerel = TemporalQuery.Timerel.AFTER,
            time = now.minusHours(1)
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 10 &&
                    it.all { attributeInstanceResult ->
                        attributeInstanceResult.temporalEntityAttribute == temporalEntityAttribute.id
                    }
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should not retrieve any instance if temporal entity does not match`() {
        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()).block() }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            time = now.minusHours(1)
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
    fun `it should create an attribute instance if it has a non null value or measuredValue`() {
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
    fun `it should not create an attribute instance if it has a null value and null measuredValue`() {
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
