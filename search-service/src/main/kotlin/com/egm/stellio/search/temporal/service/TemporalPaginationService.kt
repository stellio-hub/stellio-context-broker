package com.egm.stellio.search.temporal.service

import com.egm.stellio.search.temporal.model.AggregatedAttributeInstanceResult
import com.egm.stellio.search.temporal.model.AttributeInstanceResult
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.util.AttributesWithInstances
import java.time.ZonedDateTime

typealias Range = Pair<ZonedDateTime, ZonedDateTime>

object TemporalPaginationService {

    fun getPaginatedAttributeWithInstancesAndRange(
        attributesWithInstances: AttributesWithInstances,
        query: TemporalEntitiesQuery,
    ): Pair<AttributesWithInstances, Range?> {
        val temporalQuery = query.temporalQuery
        if (temporalQuery.isLastNTheLimit()) {
            return attributesWithInstances to null
        }

        val attributeInstancesWhoReachedLimit = getAttributesWhoReachedLimit(attributesWithInstances, query)

        if (attributeInstancesWhoReachedLimit.isEmpty()) {
            return attributesWithInstances to null
        }

        val range = getTemporalPaginationRange(attributeInstancesWhoReachedLimit, query)
        val paginatedAttributesWithInstances = filterInRange(attributesWithInstances, range)

        return paginatedAttributesWithInstances to range
    }

    private fun getAttributesWhoReachedLimit(
        attributesWithInstances: AttributesWithInstances,
        query: TemporalEntitiesQuery
    ): List<List<AttributeInstanceResult>> =
        attributesWithInstances.values.filter { instances ->
            instances.size >= query.temporalQuery.instanceLimit
        }

    private fun getTemporalPaginationRange(
        attributeInstancesWhoReachedLimit: List<List<AttributeInstanceResult>>,
        query: TemporalEntitiesQuery
    ): Range {
        val temporalQuery = query.temporalQuery

        val attributesTimeRanges = attributeInstancesWhoReachedLimit.map {
            it.first().getComparableTime() to if (query.withAggregatedValues) {
                val lastInstance = it.last() as AggregatedAttributeInstanceResult
                lastInstance.values.first().endDateTime
            } else {
                it.last().getComparableTime()
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
        attributesWithInstances: AttributesWithInstances,
        range: Range,
    ): AttributesWithInstances =
        attributesWithInstances.mapValues { (_, value) ->
            value.filter { range.contain(it.getComparableTime()) }
        }

    private fun Range.contain(time: ZonedDateTime): Boolean =
        this.first >= time && time >= this.second || this.first <= time && time <= this.second
}
