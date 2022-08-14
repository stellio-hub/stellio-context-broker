package com.egm.stellio.search.service

import arrow.core.Either
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.util.execute
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.fail
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID
import kotlin.random.Random

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class AttributeInstanceServiceTests : WithTimescaleContainer, WithKafkaContainer {

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
            attributeName = INCOMING_COMPACT_PROPERTY,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE,
            payload = EMPTY_PAYLOAD
        )

        createTemporalEntityAttribute(temporalEntityAttribute)
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
    fun `it should retrieve a full instance if temporalValues are not asked for`() = runTest {
        val observation = gimmeAttributeInstance().copy(
            time = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation)

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1)
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .singleElement()
            .hasFieldOrPropertyWithValue("temporalEntityAttribute", temporalEntityAttribute.id)
    }

    @Test
    fun `it should retrieve an instance having the corresponding time property value`() = runTest {
        val observation = gimmeAttributeInstance().copy(
            timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
            time = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation)

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            timeproperty = AttributeInstance.TemporalProperty.CREATED_AT
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .singleElement()
    }

    @Test
    fun `it should retrieve an instance with audit info if time property is not observedAt`() = runTest {
        val observation = gimmeAttributeInstance().copy(
            timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
            time = now,
            measuredValue = 12.4,
            sub = "sub"
        )
        attributeInstanceService.create(observation)

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            timeproperty = AttributeInstance.TemporalProperty.CREATED_AT
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .singleElement()
            .hasFieldOrPropertyWithValue("sub", "sub")
    }

    @Test
    fun `it should not retrieve an instance not having the corresponding time property value`() = runTest {
        val observation = gimmeAttributeInstance().copy(
            timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
            time = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation)

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            timeproperty = AttributeInstance.TemporalProperty.MODIFIED_AT
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .isEmpty()
    }

    @Test
    fun `it should retrieve all full instances if temporalValues are not asked for`() = runTest {
        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()) }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1)
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .hasSize(10)
    }

    @Test
    fun `it should retrieve all instances when no timerel and time parameters are provided`() = runTest {
        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()) }

        val enrichedEntity = attributeInstanceService.search(TemporalQuery(), temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .hasSize(10)
    }

    @Test
    fun `it should retrieve instances of a temporal entity attribute whose value type is Any`() = runTest {
        val temporalEntityAttribute2 = TemporalEntityAttribute(
            entityId = entityId,
            attributeName = "propWithStringValue",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY,
            payload = EMPTY_PAYLOAD
        )

        createTemporalEntityAttribute(temporalEntityAttribute2)

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
            attributeInstanceService.create(attributeInstance)
        }

        val enrichedEntity = attributeInstanceService.search(TemporalQuery(), temporalEntityAttribute2, true)

        assertThat(enrichedEntity)
            .hasSize(10)
            .allMatch {
                it.temporalEntityAttribute == temporalEntityAttribute2.id &&
                    (it as SimplifiedAttributeInstanceResult).value == "some value"
            }
    }

    @Test
    fun `it should sum all instances for a day`() = runTest {
        (1..10).forEach { _ ->
            val attributeInstance = gimmeAttributeInstance().copy(measuredValue = 1.0)
            attributeInstanceService.create(attributeInstance)
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            timeBucket = "1 day",
            aggregate = TemporalQuery.Aggregate.SUM
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .singleElement()
            .hasFieldOrPropertyWithValue("value", 10.0)
    }

    @Test
    fun `it should count all instances for a day`() = runTest {
        (1..9).forEach { _ ->
            val attributeInstance = gimmeAttributeInstance().copy(measuredValue = 1.0)
            attributeInstanceService.create(attributeInstance)
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            timeBucket = "1 day",
            aggregate = TemporalQuery.Aggregate.COUNT
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .singleElement()
            .hasFieldOrPropertyWithValue("value", 9L)
    }

    @Test
    fun `it should return min value of all instances for a day`() = runTest {
        (1..9).forEach { i ->
            val attributeInstance = gimmeAttributeInstance().copy(measuredValue = i.toDouble())
            attributeInstanceService.create(attributeInstance)
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            timeBucket = "1 day",
            aggregate = TemporalQuery.Aggregate.MIN
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .singleElement()
            .hasFieldOrPropertyWithValue("value", 1.0)
    }

    @Test
    fun `it should return max value of all instances for a day`() = runTest {
        (1..9).forEach { i ->
            val attributeInstance = gimmeAttributeInstance().copy(measuredValue = i.toDouble())
            attributeInstanceService.create(attributeInstance)
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            timeBucket = "1 day",
            aggregate = TemporalQuery.Aggregate.MAX
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .singleElement()
            .hasFieldOrPropertyWithValue("value", 9.0)
    }

    @Test
    fun `it should only return the last n aggregates asked in the temporal query`() = runTest {
        (1..10).forEachIndexed { index, _ ->
            val attributeInstance =
                gimmeAttributeInstance()
                    .copy(
                        measuredValue = 1.0,
                        time = now.minusHours(index.toLong())
                    )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(12),
            timeBucket = "2 hours",
            aggregate = TemporalQuery.Aggregate.SUM,
            lastN = 3
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .hasSize(3)
    }

    @Test
    fun `it should set the start time to the timeAt value if asking for an after timerel`() = runTest {
        (1..9).forEachIndexed { index, _ ->
            val attributeInstance =
                gimmeAttributeInstance()
                    .copy(
                        measuredValue = index.toDouble(),
                        time = ZonedDateTime.parse("2022-07-0${index + 1}T00:00:00Z")
                    )
            attributeInstanceService.create(attributeInstance)
        }
        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = ZonedDateTime.parse("2022-07-03T00:00:00Z"),
            timeBucket = "30 day",
            aggregate = TemporalQuery.Aggregate.MIN
        )

        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .singleElement()
            .hasFieldOrPropertyWithValue("time", ZonedDateTime.parse("2022-07-03T00:00:00Z"))
            .hasFieldOrPropertyWithValue("value", 3.0)
    }

    @Test
    fun `it should set the start time to the timeAt value if asking for a before timerel`() = runTest {
        (1..9).forEachIndexed { index, _ ->
            val attributeInstance =
                gimmeAttributeInstance()
                    .copy(
                        measuredValue = index.toDouble(),
                        time = ZonedDateTime.parse("2022-07-0${index + 1}T00:00:00Z")
                    )
            attributeInstanceService.create(attributeInstance)
        }
        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.BEFORE,
            timeAt = ZonedDateTime.parse("2022-07-03T00:00:00Z"),
            timeBucket = "30 day",
            aggregate = TemporalQuery.Aggregate.MAX
        )

        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .singleElement()
            .hasFieldOrPropertyWithValue("time", ZonedDateTime.parse("2022-06-03T00:00:00Z"))
            .hasFieldOrPropertyWithValue("value", 1.0)
    }

    @Test
    fun `it should set the start time to the timeAt value if asking for a between timerel`() = runTest {
        (1..9).forEachIndexed { index, _ ->
            val attributeInstance =
                gimmeAttributeInstance()
                    .copy(
                        measuredValue = index.toDouble(),
                        time = ZonedDateTime.parse("2022-07-0${index + 1}T00:00:00Z")
                    )
            attributeInstanceService.create(attributeInstance)
        }
        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.BETWEEN,
            timeAt = ZonedDateTime.parse("2022-07-03T00:00:00Z"),
            endTimeAt = ZonedDateTime.parse("2022-07-06T00:00:00Z"),
            timeBucket = "30 day",
            aggregate = TemporalQuery.Aggregate.MAX
        )

        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .singleElement()
            .hasFieldOrPropertyWithValue("time", ZonedDateTime.parse("2022-07-03T00:00:00Z"))
            .hasFieldOrPropertyWithValue("value", 4.0)
    }

    @Test
    fun `it should set the start time to the oldest value if asking for no timerel`() = runTest {
        (1..9).forEachIndexed { index, _ ->
            val attributeInstance =
                gimmeAttributeInstance()
                    .copy(
                        measuredValue = index.toDouble(),
                        time = ZonedDateTime.parse("2022-07-0${index + 1}T00:00:00Z")
                    )
            attributeInstanceService.create(attributeInstance)
        }
        val temporalQuery = TemporalQuery(
            timeBucket = "30 day",
            aggregate = TemporalQuery.Aggregate.MAX
        )

        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .singleElement()
            .hasFieldOrPropertyWithValue("time", ZonedDateTime.parse("2022-07-01T00:00:00Z"))
            .hasFieldOrPropertyWithValue("value", 8.0)
    }

    @Test
    fun `it should only return the last n instances asked in the temporal query`() = runTest {
        (1..10).forEach { _ ->
            val attributeInstance = gimmeAttributeInstance().copy(measuredValue = 1.0)
            attributeInstanceService.create(attributeInstance)
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1),
            lastN = 5
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .hasSize(5)
    }

    @Test
    fun `it should only retrieve the temporal evolution of the provided temporal entity attribute`() = runTest {
        val temporalEntityAttribute2 = TemporalEntityAttribute(
            entityId = entityId,
            attributeName = OUTGOING_COMPACT_PROPERTY,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE,
            payload = EMPTY_PAYLOAD
        )

        createTemporalEntityAttribute(temporalEntityAttribute2)

        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()) }
        (1..5).forEach { _ ->
            attributeInstanceService.create(
                gimmeAttributeInstance().copy(temporalEntityAttribute = temporalEntityAttribute2.id)
            )
        }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1)
        )
        val enrichedEntity = attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .hasSize(10)
            .allMatch {
                it.temporalEntityAttribute == temporalEntityAttribute.id
            }
    }

    @Test
    fun `it should not retrieve any instance if temporal entity does not match`() = runTest {
        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()) }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.minusHours(1)
        )
        val enrichedEntity =
            attributeInstanceService.search(temporalQuery, temporalEntityAttribute.copy(id = UUID.randomUUID()), false)

        assertThat(enrichedEntity)
            .isEmpty()
    }

    @Test
    fun `it should not retrieve any instance if there is no value in the time interval`() = runTest {
        (1..10).forEach { _ -> attributeInstanceService.create(gimmeAttributeInstance()) }

        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = now.plusHours(1)
        )
        val enrichedEntity =
            attributeInstanceService.search(temporalQuery, temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .isEmpty()
    }

    @Test
    fun `it should update an existing attribute instance with same observation date`() = runTest {
        val attributeInstance = gimmeAttributeInstance()

        attributeInstanceService.create(attributeInstance)

        val createResult = attributeInstanceService.create(attributeInstance.copy(measuredValue = 100.0))
        assertThat(createResult.isRight())

        val enrichedEntity = attributeInstanceService.search(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1)
            ),
            temporalEntityAttribute,
            true
        )

        assertThat(enrichedEntity)
            .singleElement()
            .matches {
                (it as SimplifiedAttributeInstanceResult).value == 100.0
            }
    }

    @Test
    fun `it should create an attribute instance if it has a non null value`() = runTest {
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
        )

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
    }

    @Test
    fun `it should not create an attribute instance if it has a null value and null measuredValue`() = runTest {
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
    fun `it should not create an attribute instance if it has no observedAt property`() = runTest {
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
    fun `it should delete attribute instance`() = runTest {
        val attributeInstance = gimmeAttributeInstance()
        attributeInstanceService.create(attributeInstance).shouldSucceed()

        attributeInstanceService.deleteEntityAttributeInstance(
            temporalEntityAttribute.entityId,
            temporalEntityAttribute.attributeName,
            attributeInstance.instanceId
        ).shouldSucceed()

        val enrichedEntity =
            attributeInstanceService.search(TemporalQuery(), temporalEntityAttribute, false)

        assertThat(enrichedEntity)
            .isEmpty()
    }

    @Test
    fun `it should not delete attribute instance if instance is not found`() = runTest {
        val attributeInstanceId = "urn:ngsi-ld:Instance:01".toUri()
        attributeInstanceService.deleteEntityAttributeInstance(
            temporalEntityAttribute.entityId,
            temporalEntityAttribute.attributeName,
            attributeInstanceId
        ).fold(
            { assertEquals("Instance $attributeInstanceId was not found", it.message) },
            { fail("The referred resource should have not been found") }
        )
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

    private fun createTemporalEntityAttribute(
        temporalEntityAttribute: TemporalEntityAttribute
    ): Either<APIException, Unit> =
        runBlocking {
            databaseClient.sql(
                """
                INSERT INTO temporal_entity_attribute 
                    (id, entity_id, attribute_name, attribute_type, attribute_value_type, created_at)
                VALUES 
                    (:id, :entity_id, :attribute_name, :attribute_type, :attribute_value_type, :created_at)
                """.trimIndent()
            )
                .bind("id", temporalEntityAttribute.id)
                .bind("entity_id", temporalEntityAttribute.entityId)
                .bind("attribute_name", temporalEntityAttribute.attributeName)
                .bind("attribute_type", temporalEntityAttribute.attributeType.toString())
                .bind("attribute_value_type", temporalEntityAttribute.attributeValueType.toString())
                .bind("created_at", temporalEntityAttribute.createdAt)
                .execute()
        }
}
