package com.egm.stellio.search.service

import com.egm.stellio.search.model.AggregatedAttributeInstanceResult
import com.egm.stellio.search.model.AttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalQuery
import java.time.ZonedDateTime

typealias Range = Pair<ZonedDateTime, ZonedDateTime>

object TemporalPaginationService {

    fun getRangeAndPaginatedTEA(
        teaWithInstances: TEAWithInstances,
        query: TemporalEntitiesQuery,
    ): Pair<TEAWithInstances, Range?> {
        val temporalQuery = query.temporalQuery
        if (temporalQuery.isLastNInsideLimit()) {
            return teaWithInstances to null
        }

        val attributeInstancesWhoReachedLimit = getAttributesWhoReachedLimit(teaWithInstances, query)

        if (attributeInstancesWhoReachedLimit.isEmpty()) {
            return teaWithInstances to null
        }

        val range = getTemporalPaginationRange(attributeInstancesWhoReachedLimit, query)
        val paginatedTEAWithinstances = filterInRange(teaWithInstances, range)

        return paginatedTEAWithinstances to range
    }

    private fun getAttributesWhoReachedLimit(
        teaWithInstances: TEAWithInstances,
        query: TemporalEntitiesQuery
    ): List<List<AttributeInstanceResult>> =
        teaWithInstances.values.filter { instances ->
            instances.size >= query.temporalQuery.instanceLimit
        }

    private fun getTemporalPaginationRange(
        attributeInstancesWhoReachedLimit: List<List<AttributeInstanceResult>>,
        query: TemporalEntitiesQuery
    ): Range {
        val temporalQuery = query.temporalQuery
        val limit = temporalQuery.instanceLimit

        val attributesTimeRanges = attributeInstancesWhoReachedLimit.map {
            it[0].getComparableTime() to if (query.withAggregatedValues) {
                val lastInstance = it[limit - 1] as AggregatedAttributeInstanceResult
                lastInstance.values.first().endDateTime
            } else {
                it[limit - 1].getComparableTime()
            }
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
        teaWithInstances: TEAWithInstances,
        range: Range,
    ): TEAWithInstances =
        teaWithInstances.mapValues { (_, value) ->
            value.filter { range.contain(it.getComparableTime()) }
        }

    private fun Range.contain(time: ZonedDateTime): Boolean =
        (this.first >= time && time >= this.second) || (this.first <= time && time <= this.second)
}
