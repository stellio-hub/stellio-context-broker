package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.search.support.*
import com.egm.stellio.shared.model.OperationNotSupportedException
import com.egm.stellio.shared.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.AbstractObjectAssert
import org.assertj.core.api.Assertions
import org.assertj.core.api.InstanceOfAssertFactories
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import java.time.LocalDate
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class AggregatedQueryServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Autowired
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val now = ngsiLdDateTime()
    private val teaUuid = UUID.randomUUID()
    private val entityId = "urn:ngsi-ld:BeeHive:${UUID.randomUUID()}".toUri()

    @AfterEach
    fun clearAttributesInstances() {
        r2dbcEntityTemplate.delete(AttributeInstance::class.java)
            .all()
            .block()

        r2dbcEntityTemplate.databaseClient
            .sql("delete from attribute_instance_audit")
            .fetch()
            .rowsUpdated()
            .block()

        r2dbcEntityTemplate.delete(TemporalEntityAttribute::class.java)
            .all()
            .block()
    }

    @ParameterizedTest
    @CsvSource(
        "totalCount, 10",
        "distinctCount, 10",
        "sum, 55.0",
        "avg, 5.5",
        "min, 1.0",
        "max, 10.0",
        "stddev, 3.0276503540974917",
        "sumsq, 385.0"
    )
    fun `it should correctly aggregate on JSON Number values`(aggrMethod: String, expectedValue: String) = runTest {
        val temporalEntityAttribute = createTemporalEntityAttribute(TemporalEntityAttribute.AttributeValueType.NUMBER)
        (1..10).forEach { i ->
            val attributeInstance = gimmeAttributeInstance(teaUuid)
                .copy(measuredValue = i.toDouble())
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, temporalEntityAttribute, now)
            .shouldSucceedWith { results ->
                assertAggregatedResult(results, aggrMethod)
                    .matches({
                        it.toString() == expectedValue
                    }, "expected value is $expectedValue")
            }
    }

    @ParameterizedTest
    @CsvSource(
        "totalCount, 10",
        "distinctCount, 10",
        "sum, ''",
        "avg, ''",
        "min, a1",
        "max, a9",
        "stddev, ''",
        "sumsq, ''"
    )
    fun `it should correctly aggregate on JSON String values`(aggrMethod: String, expectedValue: String?) = runTest {
        val temporalEntityAttribute = createTemporalEntityAttribute(TemporalEntityAttribute.AttributeValueType.STRING)
        (1..10).forEach { i ->
            val attributeInstance = gimmeAttributeInstance(teaUuid)
                .copy(measuredValue = null, value = "a$i")
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, temporalEntityAttribute, now)
            .shouldSucceedWith { results ->
                assertAggregatedResult(results, aggrMethod)
                    .matches({
                        it.toString() == expectedValue
                    }, "expected value is $expectedValue")
            }
    }

    @ParameterizedTest
    @CsvSource(
        "totalCount, 10",
        "distinctCount, 10",
        "sum, ''",
        "avg, ''",
        "min, ''",
        "max, ''",
        "stddev, ''",
        "sumsq, ''"
    )
    fun `it should correctly aggregate on JSON Object values`(aggrMethod: String, expectedValue: String?) = runTest {
        val temporalEntityAttribute = createTemporalEntityAttribute(TemporalEntityAttribute.AttributeValueType.OBJECT)
        (1..10).forEach { i ->
            val attributeInstance = gimmeAttributeInstance(teaUuid)
                .copy(
                    measuredValue = null,
                    value = """
                    {
                        "a": $i
                    }
                    """.trimIndent()
                )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, temporalEntityAttribute, now)
            .shouldSucceedWith { results ->
                assertAggregatedResult(results, aggrMethod)
                    .matches({
                        it.toString() == expectedValue
                    }, "expected value is $expectedValue")
            }
    }

    @ParameterizedTest
    @CsvSource(
        "totalCount, 10",
        "distinctCount, 10",
        "sum, 10",
        "avg, 1.00000",
        "min, 1",
        "max, 1",
        "stddev, ''",
        "sumsq, ''"
    )
    fun `it should correctly aggregate on JSON Array values`(aggrMethod: String, expectedValue: String?) = runTest {
        val temporalEntityAttribute = createTemporalEntityAttribute(TemporalEntityAttribute.AttributeValueType.ARRAY)
        (1..10).forEach { i ->
            val attributeInstance = gimmeAttributeInstance(teaUuid)
                .copy(
                    measuredValue = null,
                    value = """
                    [ $i ]
                    """.trimIndent()
                )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, temporalEntityAttribute, now)
            .shouldSucceedWith { results ->
                assertAggregatedResult(results, aggrMethod)
                    .matches({
                        it.toString() == expectedValue
                    }, "expected value is $expectedValue")
            }
    }

    @ParameterizedTest
    @CsvSource(
        "totalCount, 10",
        "distinctCount, 2",
        "sum, 5",
        "avg, 0.50000",
        "min, 0",
        "max, 1",
        "stddev, 0.52704627669472988867",
        "sumsq, 5.0"
    )
    fun `it should correctly aggregate on JSON Boolean values`(aggrMethod: String, expectedValue: String?) = runTest {
        val temporalEntityAttribute = createTemporalEntityAttribute(TemporalEntityAttribute.AttributeValueType.BOOLEAN)
        (1..10).forEach { i ->
            val attributeInstance = gimmeAttributeInstance(teaUuid)
                .copy(
                    measuredValue = null,
                    value = if (i % 2 == 0) "true" else "false"
                )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, temporalEntityAttribute, now)
            .shouldSucceedWith { results ->
                assertAggregatedResult(results, aggrMethod)
                    .matches({
                        it.toString() == expectedValue
                    }, "expected value is $expectedValue")
            }
    }

    @ParameterizedTest
    @CsvSource(
        "totalCount, 10",
        "distinctCount, 10",
        "sum, ''",
        "avg, ''",
        "min, 2023-03-05T01:01:01Z",
        "max, 2023-03-05T10:01:01Z",
        "stddev, ''",
        "sumsq, ''"
    )
    fun `it should correctly aggregate on DateTime values`(aggrMethod: String, expectedValue: String?) = runTest {
        val temporalEntityAttribute = createTemporalEntityAttribute(TemporalEntityAttribute.AttributeValueType.DATETIME)
        val baseDateTime = ZonedDateTime.parse("2023-03-05T00:01:01Z")
        (1..10).forEach { i ->
            val attributeInstance = gimmeAttributeInstance(teaUuid)
                .copy(
                    measuredValue = null,
                    value = baseDateTime.plusHours(i.toLong()).toString()
                )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, temporalEntityAttribute, now)
            .shouldSucceedWith { results ->
                assertAggregatedResult(results, aggrMethod)
                    .matches({
                        it.toString() == expectedValue
                    }, "expected value is $expectedValue")
            }
    }

    @ParameterizedTest
    @CsvSource(
        "totalCount, 10",
        "distinctCount, 10",
        "sum, ''",
        "avg, ''",
        "min, 2023-03-06",
        "max, 2023-03-15",
        "stddev, ''",
        "sumsq, ''"
    )
    fun `it should correctly aggregate on Date values`(aggrMethod: String, expectedValue: String?) = runTest {
        val temporalEntityAttribute = createTemporalEntityAttribute(TemporalEntityAttribute.AttributeValueType.DATE)
        val baseDateTime = LocalDate.parse("2023-03-05")
        (1..10).forEach { i ->
            val attributeInstance = gimmeAttributeInstance(teaUuid)
                .copy(
                    measuredValue = null,
                    value = baseDateTime.plusDays(i.toLong()).toString()
                )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, temporalEntityAttribute, now)
            .shouldSucceedWith { results ->
                assertAggregatedResult(results, aggrMethod)
                    .matches({
                        it.toString() == expectedValue
                    }, "expected value is $expectedValue")
            }
    }

    @ParameterizedTest
    @CsvSource(
        "totalCount, 10",
        "distinctCount, 10",
        "sum, ''",
        "avg, 05:30:01Z",
        "min, 01:00:01Z",
        "max, 10:00:01Z",
        "stddev, ''",
        "sumsq, ''"
    )
    fun `it should correctly aggregate on Time values`(aggrMethod: String, expectedValue: String?) = runTest {
        val temporalEntityAttribute = createTemporalEntityAttribute(TemporalEntityAttribute.AttributeValueType.TIME)
        val baseDateTime = OffsetTime.parse("00:00:01Z")
        (1..10).forEach { i ->
            val attributeInstance = gimmeAttributeInstance(teaUuid)
                .copy(
                    measuredValue = null,
                    value = baseDateTime.plusHours(i.toLong()).toString()
                )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, temporalEntityAttribute, now)
            .shouldSucceedWith { results ->
                assertAggregatedResult(results, aggrMethod)
                    .matches({
                        it.toString() == expectedValue
                    }, "expected value is $expectedValue")
            }
    }

    @ParameterizedTest
    @CsvSource(
        "totalCount, 10",
        "distinctCount, 10",
        "sum, ''",
        "avg, ''",
        "min, ''",
        "max, ''",
        "stddev, ''",
        "sumsq, ''"
    )
    fun `it should correctly aggregate on URI values`(aggrMethod: String, expectedValue: String?) = runTest {
        val temporalEntityAttribute = createTemporalEntityAttribute(TemporalEntityAttribute.AttributeValueType.URI)
        (1..10).forEach { i ->
            val attributeInstance = gimmeAttributeInstance(teaUuid)
                .copy(
                    measuredValue = null,
                    value = "urn:ngsi-ld:Entity:$i"
                )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, temporalEntityAttribute, now)
            .shouldSucceedWith { results ->
                assertAggregatedResult(results, aggrMethod)
                    .matches({
                        it.toString() == expectedValue
                    }, "expected value is $expectedValue")
            }
    }

    @Test
    fun `ìt should aggregate on the whole time range if no aggrPeriodDuration is given`() = runTest {
        val temporalEntityAttribute = createTemporalEntityAttribute(TemporalEntityAttribute.AttributeValueType.NUMBER)
        (1..10).forEach { i ->
            val attributeInstance = gimmeAttributeInstance(teaUuid)
                .copy(measuredValue = i.toDouble())
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery("avg")
        attributeInstanceService.search(
            temporalEntitiesQuery.copy(
                temporalQuery = temporalEntitiesQuery.temporalQuery.copy(aggrPeriodDuration = "PT0S")
            ),
            temporalEntityAttribute,
            now
        ).shouldSucceedWith { results ->
            assertAggregatedResult(results, "avg")
                .matches({
                    it.toString() == "5.5"
                }, "expected value is 5.5")
        }
    }

    @Test
    fun `it should handle aggregates for an attribute having different types of values in history`() = runTest {
        val temporalEntityAttribute = createTemporalEntityAttribute(TemporalEntityAttribute.AttributeValueType.ARRAY)
        (1..10).forEach { i ->
            val attributeInstanceWithArrayValue = gimmeAttributeInstance(teaUuid)
                .copy(measuredValue = null, value = "[ $i ]")
            attributeInstanceService.create(attributeInstanceWithArrayValue)
            val attributeInstanceWithStringValue = gimmeAttributeInstance(teaUuid)
                .copy(measuredValue = null, value = "$i")
            attributeInstanceService.create(attributeInstanceWithStringValue)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery("max")
        attributeInstanceService.search(temporalEntitiesQuery, temporalEntityAttribute, now)
            .shouldFail {
                assertInstanceOf(OperationNotSupportedException::class.java, it)
                assertEquals(INCONSISTENT_VALUES_IN_AGGREGATION_MESSAGE, it.message)
            }
    }

    private suspend fun createTemporalEntityAttribute(
        attributeValueType: TemporalEntityAttribute.AttributeValueType
    ): TemporalEntityAttribute {
        val temporalEntityAttribute = TemporalEntityAttribute(
            id = teaUuid,
            entityId = entityId,
            attributeName = INCOMING_PROPERTY,
            attributeValueType = attributeValueType,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )
        temporalEntityAttributeService.create(temporalEntityAttribute)
        return temporalEntityAttribute
    }

    private fun createTemporalEntitiesQuery(aggrMethod: String): TemporalEntitiesQuery =
        gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1),
                aggrPeriodDuration = "P1D",
                aggrMethods = listOfNotNull(TemporalQuery.Aggregate.forMethod(aggrMethod))
            ),
            withAggregatedValues = true
        )

    private fun assertAggregatedResult(
        result: List<AttributeInstanceResult>,
        aggrMethod: String
    ): AbstractObjectAssert<*, Any> =
        Assertions.assertThat(result)
            .singleElement()
            .asInstanceOf(InstanceOfAssertFactories.type(AggregatedAttributeInstanceResult::class.java))
            .extracting(AggregatedAttributeInstanceResult::values)
            .asInstanceOf(InstanceOfAssertFactories.list(AggregatedAttributeInstanceResult.AggregateResult::class.java))
            .singleElement()
            .hasFieldOrPropertyWithValue("aggregate", TemporalQuery.Aggregate.forMethod(aggrMethod))
            .extracting(AggregatedAttributeInstanceResult.AggregateResult::value)
}
