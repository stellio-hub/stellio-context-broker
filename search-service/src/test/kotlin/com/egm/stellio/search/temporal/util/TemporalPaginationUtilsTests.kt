package com.egm.stellio.search.temporal.util

import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.NGSILD_OBSERVED_AT_TERM
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.OUTGOING_TERM
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import java.time.ZonedDateTime

@ActiveProfiles("test")
class TemporalPaginationUtilsTests {

    private val timeAt = ZonedDateTime.parse("2019-01-01T00:00:00Z")
    private val endTimeAt = ZonedDateTime.parse("2021-01-01T00:00:00Z")
    private val leastRecentTimestamp = ZonedDateTime.parse("2020-01-01T00:01:00Z")
    private val mostRecentTimestamp = leastRecentTimestamp.plusMinutes(4) // from discrimination attribute
    private val entityId = "urn:ngsi-ld:BeeHive:TESTC"

    private fun getQuery(
        temporalQuery: TemporalQuery,
        temporalRepresentation: TemporalRepresentation = TemporalRepresentation.NORMALIZED
    ): TemporalEntitiesQuery =
        TemporalEntitiesQueryFromGet(
            temporalQuery = temporalQuery,
            entitiesQuery = EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(limit = 0, offset = 50),
                contexts = APIC_COMPOUND_CONTEXTS
            ),
            temporalRepresentation = temporalRepresentation,
            withAudit = false
        )

    private fun normalizedInstance(timestamp: ZonedDateTime): Map<String, Any> =
        mapOf(NGSILD_TYPE_TERM to "Property", NGSILD_OBSERVED_AT_TERM to timestamp.toString())

    private fun simplifiedInstance(timestamp: ZonedDateTime): List<Any> =
        listOf(1, timestamp.toString())

    private fun aggregatedInstance(start: ZonedDateTime, end: ZonedDateTime): List<Any> =
        listOf(1, start.toString(), end.toString())

    @Test
    fun `calculateRangeFromCompactedEntity should return null when lastN is the limit`() {
        val entity = mapOf(
            NGSILD_ID_TERM to entityId,
            INCOMING_TERM to (0..4).map { normalizedInstance(leastRecentTimestamp.plusMinutes(it.toLong())) }
        )
        val query = getQuery(buildDefaultTestTemporalQuery(instanceLimit = 5, lastN = 5))
        Assertions.assertNull(TemporalPaginationUtils.calculateRangeFromEntity(entity, query))
    }

    @Test
    fun `calculateRangeFromCompactedEntity should return null when no attribute reaches instanceLimit`() {
        val entity = mapOf(
            NGSILD_ID_TERM to entityId,
            INCOMING_TERM to listOf(normalizedInstance(leastRecentTimestamp))
        )
        val query = getQuery(buildDefaultTestTemporalQuery(instanceLimit = 5))
        Assertions.assertNull(TemporalPaginationUtils.calculateRangeFromEntity(entity, query))
    }

    @Test
    fun `calculateRangeFromCompactedEntity should return null for an empty entity`() {
        val entity = mapOf(NGSILD_ID_TERM to entityId, NGSILD_TYPE_TERM to "BeeHive")
        val query = getQuery(buildDefaultTestTemporalQuery(instanceLimit = 5))
        Assertions.assertNull(TemporalPaginationUtils.calculateRangeFromEntity(entity, query))
    }

    @Test
    fun `calculateRangeFromCompactedEntity should return range for NORMALIZED when instanceLimit is reached`() {
        val entity = mapOf(
            NGSILD_ID_TERM to entityId,
            INCOMING_TERM to (0..4).map { normalizedInstance(leastRecentTimestamp.plusMinutes(it.toLong())) }
        )
        val query = getQuery(buildDefaultTestTemporalQuery(instanceLimit = 5))
        val range = TemporalPaginationUtils.calculateRangeFromEntity(entity, query)

        Assertions.assertNotNull(range)
        Assertions.assertEquals(leastRecentTimestamp, range!!.first)
        Assertions.assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `calculateRangeFromCompactedEntity should return timeAt as range-start for AFTER timerel`() {
        val entity = mapOf(
            NGSILD_ID_TERM to entityId,
            INCOMING_TERM to (0..4).map { normalizedInstance(leastRecentTimestamp.plusMinutes(it.toLong())) }
        )
        val query = getQuery(
            buildDefaultTestTemporalQuery(instanceLimit = 5, timerel = TemporalQuery.Timerel.AFTER, timeAt = timeAt)
        )
        val range = TemporalPaginationUtils.calculateRangeFromEntity(entity, query)

        Assertions.assertNotNull(range)
        Assertions.assertEquals(timeAt, range!!.first)
        Assertions.assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `calculateRangeFromCompactedEntity should return timeAt as range-start for BETWEEN timerel`() {
        val entity = mapOf(
            NGSILD_ID_TERM to entityId,
            INCOMING_TERM to (0..4).map { normalizedInstance(leastRecentTimestamp.plusMinutes(it.toLong())) }
        )
        val query = getQuery(
            buildDefaultTestTemporalQuery(
                instanceLimit = 5,
                timerel = TemporalQuery.Timerel.BETWEEN,
                timeAt = timeAt,
                endTimeAt = endTimeAt
            )
        )
        val range = TemporalPaginationUtils.calculateRangeFromEntity(entity, query)

        Assertions.assertNotNull(range)
        Assertions.assertEquals(timeAt, range!!.first)
        Assertions.assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `calculateRangeFromCompactedEntity should return min timestamp as range-start for BEFORE timerel`() {
        val entity = mapOf(
            NGSILD_ID_TERM to entityId,
            INCOMING_TERM to (0..4).map { normalizedInstance(leastRecentTimestamp.plusMinutes(it.toLong())) }
        )
        val query = getQuery(
            buildDefaultTestTemporalQuery(instanceLimit = 5, timerel = TemporalQuery.Timerel.BEFORE, timeAt = endTimeAt)
        )
        val range = TemporalPaginationUtils.calculateRangeFromEntity(entity, query)

        Assertions.assertNotNull(range)
        Assertions.assertEquals(leastRecentTimestamp, range!!.first)
        Assertions.assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `calculateRangeFromCompactedEntity should derive range from the attribute that reached the limit`() {
        val entity = mapOf(
            NGSILD_ID_TERM to entityId,
            INCOMING_TERM to (0..4).map { normalizedInstance(leastRecentTimestamp.plusMinutes(it.toLong())) },
            OUTGOING_TERM to listOf(normalizedInstance(leastRecentTimestamp))
        )
        val query = getQuery(buildDefaultTestTemporalQuery(instanceLimit = 5))
        val range = TemporalPaginationUtils.calculateRangeFromEntity(entity, query)

        Assertions.assertNotNull(range)
        // range is (min of all timestamps, max of all timestamps)
        Assertions.assertEquals(leastRecentTimestamp, range!!.first)
        Assertions.assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `calculateRangeFromCompactedEntity should return range for TEMPORAL_VALUES when instanceLimit is reached`() {
        val entity = mapOf(
            NGSILD_ID_TERM to entityId,
            INCOMING_TERM to mapOf(
                NGSILD_TYPE_TERM to "Property",
                "values" to (0..4).map { simplifiedInstance(leastRecentTimestamp.plusMinutes(it.toLong())) }
            )
        )
        val query = getQuery(buildDefaultTestTemporalQuery(instanceLimit = 5), TemporalRepresentation.TEMPORAL_VALUES)
        val range = TemporalPaginationUtils.calculateRangeFromEntity(entity, query)

        Assertions.assertNotNull(range)
        Assertions.assertEquals(leastRecentTimestamp, range!!.first)
        Assertions.assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `calculateRangeFromCompactedEntity should return range for TEMPORAL_VALUES with multiple datasetIds`() {
        val entity = mapOf(
            NGSILD_ID_TERM to entityId,
            INCOMING_TERM to listOf(
                mapOf(
                    NGSILD_TYPE_TERM to "Property",
                    "datasetId" to "urn:ngsi-ld:Dataset:A",
                    "values" to (0..4).map { simplifiedInstance(leastRecentTimestamp.plusMinutes(it.toLong())) }
                ),
                mapOf(
                    NGSILD_TYPE_TERM to "Property",
                    "datasetId" to "urn:ngsi-ld:Dataset:B",
                    "values" to listOf(simplifiedInstance(leastRecentTimestamp))
                )
            )
        )
        val query = getQuery(buildDefaultTestTemporalQuery(instanceLimit = 5), TemporalRepresentation.TEMPORAL_VALUES)
        val range = TemporalPaginationUtils.calculateRangeFromEntity(entity, query)

        Assertions.assertNotNull(range)
        Assertions.assertEquals(leastRecentTimestamp, range!!.first)
        Assertions.assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `calculateRangeFromCompactedEntity should return range for AGGREGATED_VALUES when instanceLimit is reached`() {
        val entity = mapOf(
            NGSILD_ID_TERM to entityId,
            INCOMING_TERM to mapOf(
                NGSILD_TYPE_TERM to "Property",
                "min" to (0..4).map {
                    aggregatedInstance(
                        leastRecentTimestamp.plusMinutes(it.toLong()),
                        leastRecentTimestamp.plusMinutes(it.toLong() + 1)
                    )
                }
            )
        )
        val query = getQuery(buildDefaultTestTemporalQuery(instanceLimit = 5), TemporalRepresentation.AGGREGATED_VALUES)
        val range = TemporalPaginationUtils.calculateRangeFromEntity(entity, query)

        Assertions.assertNotNull(range)
        Assertions.assertEquals(leastRecentTimestamp, range!!.first)
        Assertions.assertEquals(mostRecentTimestamp, range.second)
    }

    @Test
    fun `calculateRangeFromCompactedEntities should return null when lastN is the limit`() {
        val entities = listOf(
            mapOf(
                NGSILD_ID_TERM to entityId,
                INCOMING_TERM to (0..4).map { normalizedInstance(leastRecentTimestamp.plusMinutes(it.toLong())) }
            )
        )
        val query = getQuery(buildDefaultTestTemporalQuery(instanceLimit = 5, lastN = 5))

        Assertions.assertNull(TemporalPaginationUtils.calculateRangeFromEntities(entities, query))
    }

    @Test
    fun `calculateRangeFromCompactedEntities should return null when no attribute reaches instanceLimit`() {
        val entities = listOf(
            mapOf(NGSILD_ID_TERM to entityId, INCOMING_TERM to listOf(normalizedInstance(leastRecentTimestamp)))
        )
        val query = getQuery(buildDefaultTestTemporalQuery(instanceLimit = 5))

        Assertions.assertNull(TemporalPaginationUtils.calculateRangeFromEntities(entities, query))
    }

    @Test
    fun `calculateRangeFromCompactedEntities should use earliest rangeEnd across entities`() {
        val earlierTimestamp = leastRecentTimestamp.minusMinutes(10)
        val laterTimestamp = mostRecentTimestamp.plusMinutes(10)
        val entity1 = mapOf(
            NGSILD_ID_TERM to entityId,
            INCOMING_TERM to (0..4).map { normalizedInstance(leastRecentTimestamp.plusMinutes(it.toLong())) }
        )
        val entity2 = mapOf(
            NGSILD_ID_TERM to "urn:ngsi-ld:BeeHive:TESTD",
            INCOMING_TERM to listOf(
                normalizedInstance(earlierTimestamp),
                normalizedInstance(leastRecentTimestamp),
                normalizedInstance(leastRecentTimestamp.plusMinutes(1)),
                normalizedInstance(leastRecentTimestamp.plusMinutes(2)),
                normalizedInstance(laterTimestamp),
            )
        )
        val query = getQuery(buildDefaultTestTemporalQuery(instanceLimit = 5))
        val range = TemporalPaginationUtils.calculateRangeFromEntities(listOf(entity1, entity2), query)

        Assertions.assertNotNull(range)
        Assertions.assertEquals(earlierTimestamp, range!!.first)
        Assertions.assertEquals(mostRecentTimestamp, range.second)
    }
}
