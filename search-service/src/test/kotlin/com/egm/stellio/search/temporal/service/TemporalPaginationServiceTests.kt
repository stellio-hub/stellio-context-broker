package com.egm.stellio.search.temporal.service

import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.search.entity.model.TemporalEntityAttribute
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.temporal.model.*
import com.egm.stellio.search.temporal.service.TemporalPaginationService.getRangeAndPaginatedTEA
import com.egm.stellio.search.temporal.util.TemporalEntityAttributeInstancesResult
import com.egm.stellio.shared.model.PaginationQuery
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.INCOMING_PROPERTY
import com.egm.stellio.shared.util.OUTGOING_PROPERTY
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.time.ZonedDateTime
import java.time.ZonedDateTime.now
import java.util.UUID

@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [TemporalPaginationService::class])
@EnableConfigurationProperties(SearchProperties::class)
class TemporalPaginationServiceTests {

    private val timeAt = ZonedDateTime.parse("2019-01-01T00:00:00Z")
    private val endTimeAt = ZonedDateTime.parse("2021-01-01T00:00:00Z")
    private val leastRecentTimestamp = ZonedDateTime.parse("2020-01-01T00:01:00Z")
    private val mostRecentTimestamp = leastRecentTimestamp.plusMinutes(4) // from discrimination attribute
    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    private val teaIncoming = TemporalEntityAttribute(
        entityId = entityUri,
        attributeName = INCOMING_PROPERTY,
        attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
        createdAt = now(),
        payload = EMPTY_JSON_PAYLOAD
    )

    private val teaOutgoing = TemporalEntityAttribute(
        entityId = entityUri,
        attributeName = OUTGOING_PROPERTY,
        attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
        createdAt = now(),
        payload = EMPTY_JSON_PAYLOAD
    )

    private fun getInstance(time: ZonedDateTime): AttributeInstanceResult {
        return SimplifiedAttributeInstanceResult(value = 1, time = time, temporalEntityAttribute = UUID.randomUUID())
    }

    private val teaWithInstances: TemporalEntityAttributeInstancesResult = mapOf(
        teaIncoming to listOf(
            getInstance(leastRecentTimestamp),
            getInstance(leastRecentTimestamp.plusMinutes(1)),
            getInstance(leastRecentTimestamp.plusMinutes(2)),
            getInstance(leastRecentTimestamp.plusMinutes(3)),
            getInstance(leastRecentTimestamp.plusMinutes(4)),
        ),
        teaOutgoing to listOf(
            getInstance(leastRecentTimestamp.plusMinutes(3)),
            getInstance(leastRecentTimestamp.plusMinutes(4)),
            getInstance(leastRecentTimestamp.plusMinutes(5)),
            getInstance(leastRecentTimestamp.plusMinutes(6)),
            getInstance(leastRecentTimestamp.plusMinutes(7)),
        )
    )

    private val teaWithInstancesForLastN: TemporalEntityAttributeInstancesResult = mapOf(
        teaIncoming to listOf(
            getInstance(leastRecentTimestamp),
            getInstance(leastRecentTimestamp.plusMinutes(1)),
            getInstance(leastRecentTimestamp.plusMinutes(2)),
            getInstance(leastRecentTimestamp.plusMinutes(3)),
            getInstance(leastRecentTimestamp.plusMinutes(4)),
        ),
        teaOutgoing to listOf(
            getInstance(leastRecentTimestamp.minusMinutes(3)),
            getInstance(leastRecentTimestamp.minusMinutes(2)),
            getInstance(leastRecentTimestamp.minusMinutes(1)),
            getInstance(leastRecentTimestamp),
            getInstance(leastRecentTimestamp.plusMinutes(1))
        )
    )

    private val aggregationInstances = listOf(
        AggregatedAttributeInstanceResult(
            temporalEntityAttribute = UUID.randomUUID(),
            values = listOf(
                AggregatedAttributeInstanceResult.AggregateResult(
                    TemporalQuery.Aggregate.SUM,
                    1,
                    leastRecentTimestamp,
                    leastRecentTimestamp.plusSeconds(59)
                ),
                AggregatedAttributeInstanceResult.AggregateResult(
                    TemporalQuery.Aggregate.AVG,
                    2,
                    leastRecentTimestamp,
                    leastRecentTimestamp.plusSeconds(59)
                )
            )
        ),
        AggregatedAttributeInstanceResult(
            temporalEntityAttribute = UUID.randomUUID(),
            values = listOf(
                AggregatedAttributeInstanceResult.AggregateResult(
                    TemporalQuery.Aggregate.SUM,
                    1,
                    leastRecentTimestamp.plusMinutes(1),
                    leastRecentTimestamp.plusMinutes(1).plusSeconds(59)
                ),
                AggregatedAttributeInstanceResult.AggregateResult(
                    TemporalQuery.Aggregate.AVG,
                    2,
                    leastRecentTimestamp.plusMinutes(1),
                    leastRecentTimestamp.plusMinutes(1).plusSeconds(59)
                )
            )
        )
    )

    private fun getQuery(temporalQuery: TemporalQuery): TemporalEntitiesQuery = TemporalEntitiesQuery(
        temporalQuery = temporalQuery,
        entitiesQuery = EntitiesQuery(
            paginationQuery = PaginationQuery(limit = 0, offset = 50),
            attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
            contexts = APIC_COMPOUND_CONTEXTS
        ),
        withTemporalValues = false,
        withAudit = false,
        withAggregatedValues = false
    )

    @Test
    fun `range calculation with timerel between should return range-start = timeAt`() = runTest {
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.BETWEEN,
                timeAt = timeAt,
                endTimeAt = endTimeAt
            )
        )
        val (newTeas, range) = getRangeAndPaginatedTEA(teaWithInstances, query)
        assertNotNull(range)

        assertEquals(5, newTeas[teaIncoming]?.size)
        assertEquals(2, newTeas[teaOutgoing]?.size)

        assertEquals(timeAt, range!!.first)
        assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `range calculation with timerel after should return range-start = timeAt`() = runTest {
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = timeAt,
            )
        )
        val (newTeas, range) = getRangeAndPaginatedTEA(teaWithInstances, query)

        assertNotNull(range)

        assertEquals(5, newTeas[teaIncoming]?.size)
        assertEquals(2, newTeas[teaOutgoing]?.size)

        assertEquals(timeAt, range!!.first)
        assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `range calculation with timerel before should return range-start = least recent timestamp`() = runTest {
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.BEFORE,
                timeAt = endTimeAt,
            )
        )
        val (newTeas, range) = getRangeAndPaginatedTEA(teaWithInstances, query)

        assertNotNull(range)

        assertEquals(5, newTeas[teaIncoming]?.size)
        assertEquals(2, newTeas[teaOutgoing]?.size)

        assertEquals(leastRecentTimestamp, range!!.first)
        assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `range calculation with lastN and timerel between should return range-start = endTimeAt`() = runTest {
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.BETWEEN,
                timeAt = timeAt,
                endTimeAt = endTimeAt,
                lastN = 100
            )
        )
        val (newTeas, range) = getRangeAndPaginatedTEA(teaWithInstancesForLastN, query)

        assertNotNull(range)

        assertEquals(5, newTeas[teaIncoming]?.size)
        assertEquals(2, newTeas[teaOutgoing]?.size)

        assertEquals(endTimeAt, range!!.first)
        assertEquals(leastRecentTimestamp, range.second)
    }

    @Test
    fun `range calculation with lastN and timerel after should return range-start = most recent timestamp`() = runTest {
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = timeAt,
                lastN = 100

            )
        )
        val (newTeas, range) = getRangeAndPaginatedTEA(teaWithInstancesForLastN, query)

        assertNotNull(range)

        assertEquals(5, newTeas[teaIncoming]?.size)
        assertEquals(2, newTeas[teaOutgoing]?.size)

        assertEquals(mostRecentTimestamp, range!!.first)
        assertEquals(leastRecentTimestamp, range.second)
    }

    @Test
    fun `range calculation with lastN and timerel before should return range-start = timeAt`() = runTest {
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.BEFORE,
                timeAt = endTimeAt,
                lastN = 100
            )
        )
        val (newTeas, range) = getRangeAndPaginatedTEA(teaWithInstancesForLastN, query)

        assertNotNull(range)

        assertEquals(5, newTeas[teaIncoming]?.size)
        assertEquals(2, newTeas[teaOutgoing]?.size)

        assertEquals(endTimeAt, range!!.first)
        assertEquals(leastRecentTimestamp, range.second)
    }

    @Test
    fun `range calculation with aggregatedValues`() = runTest {
        val query = TemporalEntitiesQuery(
            temporalQuery = buildDefaultTestTemporalQuery(
                instanceLimit = 2,
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = timeAt,
                aggrMethods = listOf(TemporalQuery.Aggregate.SUM, TemporalQuery.Aggregate.AVG),
                aggrPeriodDuration = "P1M"
            ),
            entitiesQuery = EntitiesQuery(
                paginationQuery = PaginationQuery(limit = 0, offset = 50),
                attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                contexts = APIC_COMPOUND_CONTEXTS
            ),
            withTemporalValues = false,
            withAudit = false,
            withAggregatedValues = true
        )

        val teaWithInstances: TemporalEntityAttributeInstancesResult =
            mapOf(teaIncoming to aggregationInstances, teaOutgoing to aggregationInstances)
        val (newTeas, range) = getRangeAndPaginatedTEA(teaWithInstances, query)

        assertNotNull(range)

        assertEquals(2, newTeas[teaIncoming]?.size)
        assertEquals(2, newTeas[teaOutgoing]?.size)

        assertEquals(leastRecentTimestamp.plusMinutes(1).plusSeconds(59), range!!.second)
        assertEquals(timeAt, range.first)
    }
}
