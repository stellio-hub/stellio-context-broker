package com.egm.stellio.search.temporal.util

import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.shared.model.COMPACTED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.model.CompactedAttributeInstance
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.TEMPORAL_REPRESENTATION_TERMS
import java.time.ZonedDateTime

typealias Range = Pair<ZonedDateTime, ZonedDateTime>

object TemporalPaginationUtils {

    fun calculateRangeFromEntities(
        entities: List<CompactedEntity>,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): Range? {
        if (temporalEntitiesQuery.temporalQuery.isLastNTheLimit() || entities.isEmpty()) return null

        val ranges = entities.mapNotNull { calculateRangeFromEntity(it, temporalEntitiesQuery) }
        if (ranges.isEmpty()) return null

        return if (!temporalEntitiesQuery.temporalQuery.hasLastN()) {
            ranges.minOf { it.first } to ranges.minOf { it.second }
        } else {
            ranges.maxOf { it.first } to ranges.maxOf { it.second }
        }
    }

    fun calculateRangeFromEntity(
        entity: CompactedEntity,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): Range? {
        if (temporalEntitiesQuery.temporalQuery.isLastNTheLimit()) return null

        val instanceLimit = temporalEntitiesQuery.temporalQuery.instanceLimit
        val outOfLimitAttributesStats = entity.entries
            .filter { (key, _) -> key !in COMPACTED_ENTITY_CORE_MEMBERS }
            .flatMap { (_, value) -> extractPerAttributeStats(value, temporalEntitiesQuery) }
            .filter { (timestamps, count) -> count >= instanceLimit && timestamps.isNotEmpty() }

        if (outOfLimitAttributesStats.isEmpty()) return null

        val outOfLimitMaxTs = outOfLimitAttributesStats.map { (timestamps, _) -> timestamps.max() }
        val outOfLimitMinTs = outOfLimitAttributesStats.map { (timestamps, _) -> timestamps.min() }

        return if (!temporalEntitiesQuery.temporalQuery.hasLastN()) {
            val rangeEnd = outOfLimitMaxTs.min()
            val rangeStart = when (temporalEntitiesQuery.temporalQuery.timerel) {
                TemporalQuery.Timerel.AFTER, TemporalQuery.Timerel.BETWEEN ->
                    temporalEntitiesQuery.temporalQuery.timeAt!!
                else -> outOfLimitMinTs.min()
            }
            rangeStart to rangeEnd
        } else {
            val discriminatingUpper = outOfLimitMaxTs.max()
            val discriminatingLower = outOfLimitMinTs.max()
            val rangeStart = when (temporalEntitiesQuery.temporalQuery.timerel) {
                TemporalQuery.Timerel.BEFORE -> temporalEntitiesQuery.temporalQuery.timeAt!!
                TemporalQuery.Timerel.BETWEEN -> temporalEntitiesQuery.temporalQuery.endTimeAt!!
                else -> discriminatingUpper
            }
            rangeStart to discriminatingLower
        }
    }

    private fun extractPerAttributeStats(
        attribute: Any,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): List<Pair<List<ZonedDateTime>, Int>> {
        val instances = when (attribute) {
            is List<*> -> attribute.filterIsInstance<CompactedAttributeInstance>()
            is Map<*, *> -> listOf(attribute as CompactedAttributeInstance)
            else -> return emptyList()
        }
        return when (temporalEntitiesQuery.temporalRepresentation) {
            TemporalRepresentation.NORMALIZED ->
                extractNormalizedStats(instances, temporalEntitiesQuery.temporalQuery.timeproperty.propertyName)
            TemporalRepresentation.TEMPORAL_VALUES ->
                extractSimplifiedOrAggregatedStats(instances, TEMPORAL_REPRESENTATION_TERMS)
            TemporalRepresentation.AGGREGATED_VALUES ->
                extractSimplifiedOrAggregatedStats(instances, TemporalQuery.Aggregate.toMethodsNames())
        }
    }

    private fun extractNormalizedStats(
        instances: List<Map<String, Any>>,
        timepropertyName: String
    ): List<Pair<List<ZonedDateTime>, Int>> {
        val timestamps = instances.mapNotNull { (it[timepropertyName] as? String)?.let(ZonedDateTime::parse) }
        return listOf(timestamps to instances.size)
    }

    private fun extractSimplifiedOrAggregatedStats(
        instances: List<Map<String, Any>>,
        keysToCountInRange: List<String>
    ): List<Pair<List<ZonedDateTime>, Int>> =
        instances.map { instance ->
            val timestamps = mutableListOf<ZonedDateTime>()
            var maxCount = 0
            for (key in keysToCountInRange) {
                val values = instance[key] as? List<*> ?: continue
                maxCount = maxOf(maxCount, values.size)
                timestamps += values.filterIsInstance<List<*>>()
                    .mapNotNull { (it.getOrNull(1) as? String)?.let(ZonedDateTime::parse) }
            }
            timestamps to maxCount
        }

    fun filterEntitiesToRange(
        entities: List<CompactedEntity>,
        range: Range,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): List<CompactedEntity> =
        entities.map { filterEntityToRange(it, range, temporalEntitiesQuery) }

    fun filterEntityToRange(
        entity: CompactedEntity,
        range: Range,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): CompactedEntity =
        entity.mapValues { (key, value) ->
            if (key in COMPACTED_ENTITY_CORE_MEMBERS) value
            else filterAttributeToRange(value, range, temporalEntitiesQuery)
        }

    private fun filterAttributeToRange(
        attribute: Any,
        range: Range,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): Any {
        val instances = when (attribute) {
            is List<*> -> attribute.filterIsInstance<CompactedAttributeInstance>()
            is Map<*, *> -> listOf(attribute as CompactedAttributeInstance)
            else -> return attribute
        }
        val filtered = when (temporalEntitiesQuery.temporalRepresentation) {
            TemporalRepresentation.NORMALIZED ->
                filterNormalizedInstances(
                    instances,
                    temporalEntitiesQuery.temporalQuery.timeproperty.propertyName,
                    range
                )
            TemporalRepresentation.TEMPORAL_VALUES ->
                filterSimplifiedOrAggregatedInstances(instances, TEMPORAL_REPRESENTATION_TERMS, range)
            TemporalRepresentation.AGGREGATED_VALUES ->
                filterSimplifiedOrAggregatedInstances(instances, TemporalQuery.Aggregate.toMethodsNames(), range)
        }
        return if (attribute is Map<*, *>) filtered.firstOrNull() ?: emptyMap<String, Any>()
        else filtered
    }

    private fun filterNormalizedInstances(
        instances: List<CompactedAttributeInstance>,
        timepropertyName: String,
        range: Range
    ): List<CompactedAttributeInstance> =
        instances.filter { instance ->
            val ts = (instance[timepropertyName] as? String)?.let(ZonedDateTime::parse)
                ?: return@filter true
            range.contains(ts)
        }

    private fun filterSimplifiedOrAggregatedInstances(
        instances: List<CompactedAttributeInstance>,
        keysToFilterOn: List<String>,
        range: Range
    ): List<CompactedAttributeInstance> =
        instances.map { instance ->
            val filteredInstance = instance.toMutableMap()
            for (key in keysToFilterOn) {
                val values = instance[key] as? List<*> ?: continue
                filteredInstance[key] = values.filterIsInstance<List<*>>().filter { value ->
                    val ts = (value.getOrNull(1) as? String)?.let(ZonedDateTime::parse)
                        ?: return@filter true
                    range.contains(ts)
                }
            }
            filteredInstance
        }

    private fun Range.contains(time: ZonedDateTime): Boolean =
        time in second..first || time in first..second
}
