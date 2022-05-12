package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import io.mockk.confirmVerified
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
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
            type = BEEHIVE_COMPACT_TYPE,
            attributeName = INCOMING_COMPACT_PROPERTY,
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

        r2dbcEntityTemplate.databaseClient
            .sql("delete from attribute_instance_audit")
            .fetch()
            .rowsUpdated()
            .block()
    }

    @Test
    fun `it should retrieve a full instance if temporalValues are not asked for`() {
        val observation = gimmeAttributeInstance().copy(
            time = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation).block()

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1)
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
    fun `it should retrieve an instance having the corresponding time property value`() {
        val observation = gimmeAttributeInstance().copy(
            timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
            time = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation).block()

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            timeproperty = AttributeInstance.TemporalProperty.CREATED_AT
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches { it.size == 1 }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve an instance with audit info if time property is not observedAt`() {
        val observation = gimmeAttributeInstance().copy(
            timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
            time = now,
            measuredValue = 12.4,
            sub = "sub"
        )
        attributeInstanceService.create(observation).block()

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            timeproperty = AttributeInstance.TemporalProperty.CREATED_AT
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 1 &&
                    it[0] is FullAttributeInstanceResult &&
                    (it[0] as FullAttributeInstanceResult).sub == "sub"
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should not retrieve an instance not having the corresponding time property value`() {
        val observation = gimmeAttributeInstance().copy(
            timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
            time = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation).block()

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            timeproperty = AttributeInstance.TemporalProperty.MODIFIED_AT
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches { it.isEmpty() }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve all full instances if temporalValues are not asked for`() {
        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()).block() }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1)
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
            type = BEEHIVE_COMPACT_TYPE,
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
                timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT,
                time = observedAt,
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
            timeAt = now.minusHours(1),
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
            timeAt = now.minusHours(1),
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
    fun `it should return min value of all instances for a day`() {
        (1..9).forEach { i ->
            val attributeInstance = gimmeAttributeInstance().copy(measuredValue = i.toDouble())
            attributeInstanceService.create(attributeInstance).block()
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            timeBucket = "1 day",
            aggregate = TemporalQuery.Aggregate.MIN
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 1 &&
                    it[0] is SimplifiedAttributeInstanceResult &&
                    (it[0] as SimplifiedAttributeInstanceResult).value as Double == 1.0
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should return max value of all instances for a day`() {
        (1..9).forEach { i ->
            val attributeInstance = gimmeAttributeInstance().copy(measuredValue = i.toDouble())
            attributeInstanceService.create(attributeInstance).block()
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            timeBucket = "1 day",
            aggregate = TemporalQuery.Aggregate.MAX
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.size == 1 &&
                    it[0] is SimplifiedAttributeInstanceResult &&
                    (it[0] as SimplifiedAttributeInstanceResult).value as Double == 9.0
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
                        time = now.minusHours(index.toLong())
                    )
            attributeInstanceService.create(attributeInstance).block()
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(12),
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
            timeAt = now.minusHours(1),
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
            type = BEEHIVE_COMPACT_TYPE,
            attributeName = OUTGOING_COMPACT_PROPERTY,
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
            timeAt = now.minusHours(1)
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
            timeAt = now.minusHours(1)
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
    fun `it should not retrieve any instance if there is no value in the time interval`() {
        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()).block() }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.plusHours(1)
        )
        val enrichedEntity =
            attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches {
                it.isEmpty()
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should update an existing attribute instance with same observation date`() {
        val attributeInstance = gimmeAttributeInstance()

        attributeInstanceService.create(attributeInstance).block()

        StepVerifier
            .create(attributeInstanceService.create(attributeInstance.copy(measuredValue = 100.0)))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(
                attributeInstanceService.search(
                    TemporalQuery(
                        timerel = TemporalQuery.Timerel.AFTER,
                        timeAt = now.minusHours(1)
                    ),
                    temporalEntityAttribute,
                    true
                )
            )
            .expectNextMatches {
                it.size == 1 &&
                    (it[0] as SimplifiedAttributeInstanceResult).value == 100.0
            }
    }

    @Test
    fun `it should create an attribute instance if it has a non null value`() {
        val attributeInstanceService = spyk(AttributeInstanceService(databaseClient), recordPrivateCalls = true)
        val attributeValues = mapOf(
            NGSILD_OBSERVED_AT_PROPERTY to listOf(
                mapOf(
                    JSONLD_VALUE_KW to "2015-10-18T11:20:30.000001Z",
                    JSONLD_TYPE to NGSILD_DATE_TIME_TYPE
                )
            ),
            NGSILD_PROPERTY_VALUE to listOf(
                mapOf(
                    JSONLD_VALUE_KW to 550.0
                )
            )
        )

        attributeInstanceService.addAttributeInstance(
            temporalEntityAttribute.id,
            OUTGOING_COMPACT_PROPERTY,
            attributeValues,
            listOf(NGSILD_CORE_CONTEXT)
        ).block()

        verify {
            attributeInstanceService["create"](
                match<AttributeInstance> {
                    it.time.toString() == "2015-10-18T11:20:30.000001Z" &&
                        it.value == null &&
                        it.measuredValue == 550.0 &&
                        it.payload.matchContent(
                            """
                            {
                                "value": 550.0, 
                                "observedAt": "2015-10-18T11:20:30.000001Z",
                                "instanceId": "${it.instanceId}"
                            }
                            """.trimIndent()
                        )
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should not create an attribute instance if it has a null value and null measuredValue`() {
        val attributeInstanceService = spyk(AttributeInstanceService(databaseClient), recordPrivateCalls = true)
        val attributeValues = mapOf(
            NGSILD_OBSERVED_AT_PROPERTY to listOf(
                mapOf(
                    JSONLD_VALUE_KW to "2015-10-18T11:20:30.000001Z",
                    JSONLD_TYPE to NGSILD_DATE_TIME_TYPE
                )
            )
        )

        val exception = assertThrows<BadRequestDataException>("It should have thrown a BadRequestDataException") {
            attributeInstanceService.addAttributeInstance(
                temporalEntityAttribute.id,
                OUTGOING_COMPACT_PROPERTY,
                attributeValues,
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
        assertEquals("Attribute outgoing has an instance without a value", exception.message)
    }

    @Test
    fun `it should not create an attribute instance if it has no observedAt property`() {
        val attributeInstanceService = spyk(AttributeInstanceService(databaseClient), recordPrivateCalls = true)
        val attributeValues = mapOf(
            NGSILD_PROPERTY_VALUE to listOf(
                mapOf(
                    JSONLD_VALUE_KW to 550.0
                )
            )
        )

        val exception = assertThrows<BadRequestDataException>("It should have thrown a BadRequestDataException") {
            attributeInstanceService.addAttributeInstance(
                temporalEntityAttribute.id,
                "outgoing",
                attributeValues,
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
        assertEquals("Attribute outgoing has an instance without an observed date", exception.message)
    }

    @Test
    fun `it should delete attribute instance`() {
        val attributeInstance = gimmeAttributeInstance()
        attributeInstanceService.create(attributeInstance).block()

        runBlocking {
            attributeInstanceService.deleteEntityAttributeInstance(
                temporalEntityAttribute.entityId,
                temporalEntityAttribute.attributeName,
                attributeInstance.instanceId
            ).fold(
                { fail("The referred resource has been found") },
                { }
            )
        }

        val enrichedEntity =
            attributeInstanceService.search(TemporalQuery(), temporalEntityAttribute, false)

        StepVerifier.create(enrichedEntity)
            .expectNextMatches { it.isEmpty() }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should not delete attribute instance if instance is not found`() {
        val attributeInstanceId = "urn:ngsi-ld:Instance:01".toUri()
        runBlocking {
            attributeInstanceService.deleteEntityAttributeInstance(
                temporalEntityAttribute.entityId,
                temporalEntityAttribute.attributeName,
                attributeInstanceId
            ).fold(
                { assertEquals("Instance $attributeInstanceId was not found", it.message) },
                { fail("The referred resource has not been found") }
            )
        }
    }

    private fun gimmeAttributeInstance(): AttributeInstance {
        val measuredValue = Random.nextDouble()
        val observedAt = Instant.now().atZone(ZoneOffset.UTC)
        return AttributeInstance(
            temporalEntityAttribute = temporalEntityAttribute.id,
            measuredValue = measuredValue,
            timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT,
            time = observedAt,
            payload = mapOf(
                "type" to "Property",
                "value" to measuredValue,
                "observedAt" to observedAt
            )
        )
    }
}
