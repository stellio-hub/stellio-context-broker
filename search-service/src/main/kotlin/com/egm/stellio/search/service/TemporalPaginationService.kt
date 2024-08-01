package com.egm.stellio.search.service

import com.egm.stellio.search.model.AggregatedAttributeInstanceResult
import com.egm.stellio.search.model.AttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalQuery
import java.time.ZonedDateTime

typealias Range = Pair<ZonedDateTime, ZonedDateTime>

object TemporalPaginationService {

    fun getRangeAndPaginatedTEA(
        teaWithInstances: TEAWithinstances,
        query: TemporalEntitiesQuery,
    ): Pair<TEAWithinstances, Range?> {
        val temporalQuery = query.temporalQuery
        if (temporalQuery.isLastNInsideLimit()) {
            return teaWithInstances to null
        }

        if (query.withAggregatedValues) {
            return getRangeAndPaginatedTEAForAggregatedValue(teaWithInstances, query)
        }

        val attributeInstancesWhoReachedLimit = getAttributesWhoReachedLimit(teaWithInstances, query)

        if (attributeInstancesWhoReachedLimit.isEmpty()) {
            return teaWithInstances to null
        }

        val range = getTemporalPaginationRange(attributeInstancesWhoReachedLimit, query)
        val paginatedTEAWithinstances = filterInRange(teaWithInstances, range)

        return paginatedTEAWithinstances to range
    }

    private fun getAttributesWhoReachedLimit(teaWithInstances: TEAWithinstances, query: TemporalEntitiesQuery):
        List<List<AttributeInstanceResult>> {
        val temporalQuery = query.temporalQuery

        return teaWithInstances.values.filter { instances ->
            instances.size >= temporalQuery.instanceLimit
        }
    }

    private fun getTemporalPaginationRange(
        attributeInstancesWhoReachedLimit: List<List<AttributeInstanceResult>>,
        query: TemporalEntitiesQuery
    ): Range {
        val temporalQuery = query.temporalQuery
        val limit = temporalQuery.instanceLimit

        val attributesTimeRanges = attributeInstancesWhoReachedLimit.map {
            it[0].getComparableTime() to it[limit - 1].getComparableTime()
        }

        if (temporalQuery.hasLastN()) {
            val discriminatingTimeRange = attributesTimeRanges.maxOf { it.second } to
                attributesTimeRanges.maxOf { it.first }
            val rangeStart = when (temporalQuery.timerel) {
                TemporalQuery.Timerel.BEFORE -> temporalQuery.timeAt!!
                TemporalQuery.Timerel.BETWEEN -> temporalQuery.endTimeAt!!
                else -> discriminatingTimeRange.first
            }
            return rangeStart to discriminatingTimeRange.second
        } else {
            val discriminatingTimeRange = attributesTimeRanges.minOf { it.first } to
                attributesTimeRanges.minOf { it.second }
            val rangeStart = when (temporalQuery.timerel) {
                TemporalQuery.Timerel.AFTER -> temporalQuery.timeAt!!
                TemporalQuery.Timerel.BETWEEN -> temporalQuery.timeAt!!
                else -> discriminatingTimeRange.first
            }
            return rangeStart to discriminatingTimeRange.second
        }
    }

    private fun filterInRange(
        teaWithInstances: TEAWithinstances,
        range: Range,
    ): TEAWithinstances {
        return teaWithInstances.mapNotNull { (key, value) ->
            return@mapNotNull key to value.filter { range.contain(it.getComparableTime()) }
        }.toMap()
    }

    private fun Range.contain(time: ZonedDateTime): Boolean =
        (this.first >= time && time >= this.second) || (this.first <= time && time <= this.second)

    private fun getRangeAndPaginatedTEAForAggregatedValue(
        teaWithInstances: TEAWithinstances,
        query: TemporalEntitiesQuery,
    ): Pair<TEAWithinstances, Range?> {
        if (teaWithInstances.values.isEmpty()) return teaWithInstances to null
        // simpler for aggregated value since all values already are synchronized.
        val firstAttributeInstanceResult = teaWithInstances.values.first().first() as AggregatedAttributeInstanceResult
        val asReachLimit = firstAttributeInstanceResult.values.size >= query.temporalQuery.instanceLimit
        if (!asReachLimit) return teaWithInstances to null

        val range = firstAttributeInstanceResult.values[0].startDateTime to
            firstAttributeInstanceResult.values.last().endDateTime
        return teaWithInstances to range
    }
}
