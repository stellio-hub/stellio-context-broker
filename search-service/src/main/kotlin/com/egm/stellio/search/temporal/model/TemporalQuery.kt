package com.egm.stellio.search.temporal.model

import java.time.ZonedDateTime

data class TemporalQuery(
    val timerel: Timerel? = null,
    val timeAt: ZonedDateTime? = null,
    val endTimeAt: ZonedDateTime? = null,
    val aggrPeriodDuration: String? = null,
    val aggrMethods: List<Aggregate>? = null,
    val lastN: Int? = null,
    val instanceLimit: Int,
    val timeproperty: AttributeInstance.TemporalProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
) {
    fun hasLastN(): Boolean = lastN != null

    fun isLastNTheLimit(): Boolean = lastN != null && instanceLimit == lastN

    enum class Timerel {
        BEFORE,
        AFTER,
        BETWEEN
    }

    enum class Aggregate(val method: String) {
        TOTAL_COUNT("totalCount"),
        DISTINCT_COUNT("distinctCount"),
        SUM("sum"),
        AVG("avg"),
        MIN("min"),
        MAX("max"),
        STDDEV("stddev"),
        SUMSQ("sumsq");

        companion object {
            fun isSupportedAggregate(method: String): Boolean =
                entries.any { it.method == method }

            fun forMethod(method: String): Aggregate? =
                entries.find { it.method == method }
        }
    }
}
