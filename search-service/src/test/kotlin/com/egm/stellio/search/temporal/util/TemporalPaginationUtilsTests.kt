package com.egm.stellio.search.temporal.util

import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.temporal.model.AggregatedAttributeInstanceResult
import com.egm.stellio.search.temporal.model.AttributeInstanceResult
import com.egm.stellio.search.temporal.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.OUTGOING_IRI
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import java.time.ZonedDateTime
import java.util.UUID

@ActiveProfiles("test")
class TemporalPaginationUtilsTests {

    private val timeAt = ZonedDateTime.parse("2019-01-01T00:00:00Z")
    private val endTimeAt = ZonedDateTime.parse("2021-01-01T00:00:00Z")
    private val leastRecentTimestamp = ZonedDateTime.parse("2020-01-01T00:01:00Z")
    private val mostRecentTimestamp = leastRecentTimestamp.plusMinutes(4) // from discrimination attribute
    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    private val attributeIncoming = Attribute(
        entityId = entityUri,
        attributeName = INCOMING_IRI,
        attributeValueType = Attribute.AttributeValueType.NUMBER,
        createdAt = ZonedDateTime.now(),
        payload = EMPTY_JSON_PAYLOAD
    )

    private val attributeOutgoing = Attribute(
        entityId = entityUri,
        attributeName = OUTGOING_IRI,
        attributeValueType = Attribute.AttributeValueType.NUMBER,
        createdAt = ZonedDateTime.now(),
        payload = EMPTY_JSON_PAYLOAD
    )

    private fun getInstance(time: ZonedDateTime): AttributeInstanceResult {
        return SimplifiedAttributeInstanceResult(value = 1, time = time, attributeUuid = UUID.randomUUID())
    }

    private val attributesWithInstances: AttributesWithInstances = mapOf(
        attributeIncoming to listOf(
            getInstance(leastRecentTimestamp),
            getInstance(leastRecentTimestamp.plusMinutes(1)),
            getInstance(leastRecentTimestamp.plusMinutes(2)),
            getInstance(leastRecentTimestamp.plusMinutes(3)),
            getInstance(leastRecentTimestamp.plusMinutes(4)),
        ),
        attributeOutgoing to listOf(
            getInstance(leastRecentTimestamp.plusMinutes(3)),
            getInstance(leastRecentTimestamp.plusMinutes(4)),
            getInstance(leastRecentTimestamp.plusMinutes(5)),
            getInstance(leastRecentTimestamp.plusMinutes(6)),
            getInstance(leastRecentTimestamp.plusMinutes(7)),
        )
    )

    private val attributesWithInstancesForLastN: AttributesWithInstances = mapOf(
        attributeIncoming to listOf(
            getInstance(leastRecentTimestamp),
            getInstance(leastRecentTimestamp.plusMinutes(1)),
            getInstance(leastRecentTimestamp.plusMinutes(2)),
            getInstance(leastRecentTimestamp.plusMinutes(3)),
            getInstance(leastRecentTimestamp.plusMinutes(4)),
        ),
        attributeOutgoing to listOf(
            getInstance(leastRecentTimestamp.minusMinutes(3)),
            getInstance(leastRecentTimestamp.minusMinutes(2)),
            getInstance(leastRecentTimestamp.minusMinutes(1)),
            getInstance(leastRecentTimestamp),
            getInstance(leastRecentTimestamp.plusMinutes(1))
        )
    )

    private val aggregationInstances = listOf(
        AggregatedAttributeInstanceResult(
            attributeUuid = UUID.randomUUID(),
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
            attributeUuid = UUID.randomUUID(),
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

    private fun getQuery(temporalQuery: TemporalQuery): TemporalEntitiesQuery = TemporalEntitiesQueryFromGet(
        temporalQuery = temporalQuery,
        entitiesQuery = EntitiesQueryFromGet(
            paginationQuery = PaginationQuery(limit = 0, offset = 50),
            attrs = setOf(INCOMING_IRI, OUTGOING_IRI),
            contexts = APIC_COMPOUND_CONTEXTS
        ),
        temporalRepresentation = TemporalRepresentation.NORMALIZED,
        withAudit = false
    )

    @Test
    fun `range calculation with timerel between should return range-start = timeAt`() {
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.BETWEEN,
                timeAt = timeAt,
                endTimeAt = endTimeAt
            )
        )
        val (newTeas, range) = TemporalPaginationUtils.getPaginatedAttributeWithInstancesAndRange(
            attributesWithInstances,
            query
        )
        Assertions.assertNotNull(range)

        Assertions.assertEquals(5, newTeas[attributeIncoming]?.size)
        Assertions.assertEquals(2, newTeas[attributeOutgoing]?.size)

        Assertions.assertEquals(timeAt, range!!.first)
        Assertions.assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `range calculation with timerel after should return range-start = timeAt`() {
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = timeAt,
            )
        )
        val (newTeas, range) = TemporalPaginationUtils.getPaginatedAttributeWithInstancesAndRange(
            attributesWithInstances,
            query
        )

        Assertions.assertNotNull(range)

        Assertions.assertEquals(5, newTeas[attributeIncoming]?.size)
        Assertions.assertEquals(2, newTeas[attributeOutgoing]?.size)

        Assertions.assertEquals(timeAt, range!!.first)
        Assertions.assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `range calculation with timerel before should return range-start = least recent timestamp`() {
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.BEFORE,
                timeAt = endTimeAt,
            )
        )
        val (newTeas, range) = TemporalPaginationUtils.getPaginatedAttributeWithInstancesAndRange(
            attributesWithInstances,
            query
        )

        Assertions.assertNotNull(range)

        Assertions.assertEquals(5, newTeas[attributeIncoming]?.size)
        Assertions.assertEquals(2, newTeas[attributeOutgoing]?.size)

        Assertions.assertEquals(leastRecentTimestamp, range!!.first)
        Assertions.assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `range calculation with lastN and timerel between should return range-start = endTimeAt`() {
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.BETWEEN,
                timeAt = timeAt,
                endTimeAt = endTimeAt,
                lastN = 100
            )
        )
        val (newTeas, range) = TemporalPaginationUtils.getPaginatedAttributeWithInstancesAndRange(
            attributesWithInstancesForLastN,
            query
        )

        Assertions.assertNotNull(range)

        Assertions.assertEquals(5, newTeas[attributeIncoming]?.size)
        Assertions.assertEquals(2, newTeas[attributeOutgoing]?.size)

        Assertions.assertEquals(endTimeAt, range!!.first)
        Assertions.assertEquals(leastRecentTimestamp, range.second)
    }

    @Test
    fun `range calculation with lastN and timerel after should return range-start = most recent timestamp`() {
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = timeAt,
                lastN = 100
            )
        )
        val (newTeas, range) = TemporalPaginationUtils.getPaginatedAttributeWithInstancesAndRange(
            attributesWithInstancesForLastN,
            query
        )

        Assertions.assertNotNull(range)

        Assertions.assertEquals(5, newTeas[attributeIncoming]?.size)
        Assertions.assertEquals(2, newTeas[attributeOutgoing]?.size)

        Assertions.assertEquals(mostRecentTimestamp, range!!.first)
        Assertions.assertEquals(leastRecentTimestamp, range.second)
    }

    @Test
    fun `range calculation with lastN and timerel before should return range-start = timeAt`() {
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.BEFORE,
                timeAt = endTimeAt,
                lastN = 100
            )
        )
        val (newAttributes, range) = TemporalPaginationUtils.getPaginatedAttributeWithInstancesAndRange(
            attributesWithInstancesForLastN,
            query
        )

        Assertions.assertNotNull(range)

        Assertions.assertEquals(5, newAttributes[attributeIncoming]?.size)
        Assertions.assertEquals(2, newAttributes[attributeOutgoing]?.size)

        Assertions.assertEquals(endTimeAt, range!!.first)
        Assertions.assertEquals(leastRecentTimestamp, range.second)
    }

    @Test
    fun `range calculation with aggregatedValues should return the computed range`() {
        val query = TemporalEntitiesQueryFromGet(
            temporalQuery = buildDefaultTestTemporalQuery(
                instanceLimit = 2,
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = timeAt,
                aggrMethods = listOf(TemporalQuery.Aggregate.SUM, TemporalQuery.Aggregate.AVG),
                aggrPeriodDuration = "P1M"
            ),
            entitiesQuery = EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(limit = 0, offset = 50),
                attrs = setOf(INCOMING_IRI, OUTGOING_IRI),
                contexts = APIC_COMPOUND_CONTEXTS
            ),
            temporalRepresentation = TemporalRepresentation.AGGREGATED_VALUES,
            withAudit = false
        )

        val attributesWithInstances: AttributesWithInstances =
            mapOf(attributeIncoming to aggregationInstances, attributeOutgoing to aggregationInstances)
        val (newAttributes, range) = TemporalPaginationUtils.getPaginatedAttributeWithInstancesAndRange(
            attributesWithInstances,
            query
        )

        Assertions.assertNotNull(range)

        Assertions.assertEquals(2, newAttributes[attributeIncoming]?.size)
        Assertions.assertEquals(2, newAttributes[attributeOutgoing]?.size)

        Assertions.assertEquals(leastRecentTimestamp.plusMinutes(1).plusSeconds(59), range!!.second)
        Assertions.assertEquals(timeAt, range.first)
    }
}
