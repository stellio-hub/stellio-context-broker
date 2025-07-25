package com.egm.stellio.search.temporal.service

import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.service.EntityAttributeService
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.support.gimmeNumericPropertyAttributeInstance
import com.egm.stellio.search.support.gimmeTemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.AggregatedAttributeInstanceResult
import com.egm.stellio.search.temporal.model.AttributeInstance
import com.egm.stellio.search.temporal.model.AttributeInstanceResult
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.shared.model.OperationNotSupportedException
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.AbstractObjectAssert
import org.assertj.core.api.Assertions
import org.assertj.core.api.InstanceOfAssertFactories
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.test.context.ActiveProfiles
import java.time.LocalDate
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.util.UUID

@SpringBootTest
@ActiveProfiles("test")
class AggregatedTemporalQueryServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Autowired
    private lateinit var entityAttributeService: EntityAttributeService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    @MockkBean(relaxed = true)
    private lateinit var searchProperties: SearchProperties

    private val now = ngsiLdDateTime()
    private val attributeUuid = UUID.randomUUID()
    private val entityId = "urn:ngsi-ld:BeeHive:${UUID.randomUUID()}".toUri()

    @BeforeEach
    fun mockSearchProperties() {
        every { searchProperties.timezoneForTimeBuckets } returns "GMT"
    }

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

        r2dbcEntityTemplate.delete<Attribute>().from("temporal_entity_attribute").all().block()
    }

    @ParameterizedTest
    @CsvSource(
        "totalCount, 10",
        "distinctCount, 10",
        "sum, 55.0",
        "avg, 5.5",
        "min, 1.0",
        "max, 10.0",
        "stddev, 3.0276503541",
        "sumsq, 385.0"
    )
    fun `it should correctly aggregate on JSON Number values`(aggrMethod: String, expectedValue: String) = runTest {
        val attribute = createAttribute(Attribute.AttributeValueType.NUMBER)
        (1..10).forEach { i ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                measuredValue = i.toDouble()
            )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, attribute, now)
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
        val attribute = createAttribute(Attribute.AttributeValueType.STRING)
        (1..10).forEach { i ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                value = "a$i"
            )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, attribute, now)
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
        val attribute = createAttribute(Attribute.AttributeValueType.OBJECT)
        (1..10).forEach { i ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                value = """{ "a": $i }"""
            )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, attribute, now)
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
        val attribute = createAttribute(Attribute.AttributeValueType.ARRAY)
        (1..10).forEach { i ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                value = """[ $i ]"""
            )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, attribute, now)
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
        val attribute = createAttribute(Attribute.AttributeValueType.BOOLEAN)
        (1..10).forEach { i ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                value = if (i % 2 == 0) "true" else "false"
            )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, attribute, now)
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
        val attribute = createAttribute(Attribute.AttributeValueType.DATETIME)
        val baseDateTime = ZonedDateTime.parse("2023-03-05T00:01:01Z")
        (1..10).forEach { i ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                value = baseDateTime.plusHours(i.toLong()).toString()
            )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, attribute, now)
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
        val attribute = createAttribute(Attribute.AttributeValueType.DATE)
        val baseDateTime = LocalDate.parse("2023-03-05")
        (1..10).forEach { i ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                value = baseDateTime.plusDays(i.toLong()).toString()
            )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, attribute, now)
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
        val attribute = createAttribute(Attribute.AttributeValueType.TIME)
        val baseDateTime = OffsetTime.parse("00:00:01Z")
        (1..10).forEach { i ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                value = baseDateTime.plusHours(i.toLong()).toString()
            )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, attribute, now)
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
        val attribute = createAttribute(Attribute.AttributeValueType.URI)
        (1..10).forEach { i ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                value = "urn:ngsi-ld:Entity:$i"
            )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery(aggrMethod)
        attributeInstanceService.search(temporalEntitiesQuery, attribute, now)
            .shouldSucceedWith { results ->
                assertAggregatedResult(results, aggrMethod)
                    .matches({
                        it.toString() == expectedValue
                    }, "expected value is $expectedValue")
            }
    }

    @Test
    fun `it should aggregate on the whole time range if no aggrPeriodDuration is given`() = runTest {
        val attribute = createAttribute(Attribute.AttributeValueType.NUMBER)
        (1..10).forEach { i ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                measuredValue = i.toDouble()
            )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery("avg")
        attributeInstanceService.search(
            temporalEntitiesQuery.copy(
                temporalQuery = temporalEntitiesQuery.temporalQuery.copy(aggrPeriodDuration = "PT0S")
            ),
            attribute,
            now
        ).shouldSucceedWith { results ->
            assertAggregatedResult(results, "avg")
                .matches({
                    it.toString() == "5.5"
                }, "expected value is 5.5")
        }
    }

    @ParameterizedTest
    @CsvSource(
        "P1D, 10",
        "PT48H, 6",
        "PT24H, 10",
        "PT12H, 10",
        "P1W, 2",
        "P2W, 1",
        "P1M, 2"
    )
    fun `it should aggregate on the asked aggrPeriodDuration`(
        aggrPeriodDuration: String,
        expectedNumberOfBuckets: Int
    ) = runTest {
        val attribute = createAttribute(Attribute.AttributeValueType.NUMBER)
        val startTimestamp = ZonedDateTime.parse("2023-12-28T12:00:00Z")
        (1..10).forEach { i ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                time = startTimestamp.plusDays(i.toLong())
            )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery("avg", aggrPeriodDuration)
        attributeInstanceService.search(
            temporalEntitiesQuery.copy(
                temporalQuery = temporalEntitiesQuery.temporalQuery.copy(timeAt = startTimestamp)
            ),
            attribute,
            startTimestamp
        )
            .shouldSucceedWith { results ->
                assertEquals(expectedNumberOfBuckets, results.size)
            }
    }

    @Test
    fun `it should handle aggregates for an attribute having different types of values in history`() = runTest {
        val attribute = createAttribute(Attribute.AttributeValueType.ARRAY)
        (1..10).forEach { i ->
            val attributeInstanceWithArrayValue = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                value = "[ $i ]"
            )
            attributeInstanceService.create(attributeInstanceWithArrayValue)
            val attributeInstanceWithStringValue = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                value = "$i"
            )
            attributeInstanceService.create(attributeInstanceWithStringValue)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery("max")
        attributeInstanceService.search(temporalEntitiesQuery, attribute, now)
            .shouldFail {
                assertInstanceOf(OperationNotSupportedException::class.java, it)
                assertEquals("cannot get array length of a scalar", it.message)
            }
    }

    @Test
    fun `it should aggregate using the specified timezone`() = runTest {
        // set the timezone to Europe/Paris to have all the results aggregated on January 2024
        every { searchProperties.timezoneForTimeBuckets } returns "Europe/Paris"

        val attribute = createAttribute(Attribute.AttributeValueType.NUMBER)
        val startTimestamp = ZonedDateTime.parse("2023-12-31T23:00:00Z")
        (0..9).forEach { i ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(
                attributeUuid = attributeUuid,
                measuredValue = 1.0,
                time = startTimestamp.plusHours(i.toLong())
            )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = createTemporalEntitiesQuery("sum", "P1M")
        attributeInstanceService.search(
            temporalEntitiesQuery.copy(
                temporalQuery = temporalEntitiesQuery.temporalQuery.copy(timeAt = startTimestamp)
            ),
            attribute,
            startTimestamp
        )
            .shouldSucceedWith { results ->
                assertEquals(1, results.size)
                assertEquals(10.0, (results[0] as AggregatedAttributeInstanceResult).values[0].value)
            }
    }

    private suspend fun createAttribute(
        attributeValueType: Attribute.AttributeValueType
    ): Attribute {
        val attribute = Attribute(
            id = attributeUuid,
            entityId = entityId,
            attributeName = INCOMING_IRI,
            attributeValueType = attributeValueType,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )
        entityAttributeService.upsert(attribute)
        return attribute
    }

    private fun createTemporalEntitiesQuery(
        aggrMethod: String,
        aggrPeriodDuration: String = "P1D"
    ): TemporalEntitiesQueryFromGet =
        gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1),
                aggrPeriodDuration = aggrPeriodDuration,
                aggrMethods = listOfNotNull(TemporalQuery.Aggregate.forMethod(aggrMethod))
            ),
            temporalRepresentation = TemporalRepresentation.AGGREGATED_VALUES
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
