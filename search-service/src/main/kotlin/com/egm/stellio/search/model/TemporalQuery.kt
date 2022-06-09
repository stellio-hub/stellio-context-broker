package com.egm.stellio.search.model

import java.time.ZonedDateTime

data class TemporalQuery(
    val expandedAttrs: Set<String> = emptySet(),
    val timerel: Timerel? = null,
    val timeAt: ZonedDateTime? = null,
    val endTimeAt: ZonedDateTime? = null,
    val aggrPeriodDuration: String? = null,
    val aggrMethods: List<Aggregate>? = null,
    val lastN: Int? = null,
    val timeproperty: AttributeInstance.TemporalProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
) {
    enum class Timerel {
        BEFORE,
        AFTER,
        BETWEEN
    }

    enum class Aggregate(val aggregate: String) {
        TOTAL_COUNT("totalCount"),
        DISTINCT_COUNT("distinctCount"),
        SUM("sum"),
        AVG("avg"),
        MIN("min"),
        MAX("max"),
        STDDEV("stddev"),
        SUMSQ("sumsq");

        companion object {
            fun isSupportedAggregate(aggregate: String): Boolean =
                values().toList().any { it.aggregate == aggregate }
        }
    }
}
