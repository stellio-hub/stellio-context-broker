package com.egm.stellio.search.model

import java.time.ZonedDateTime

const val WHOLE_TIME_RANGE_DURATION = "PT0S"

const val TIMEREL_PARAM = "timerel" // do i put them in TemporalQuery companion object?
const val TIMEAT_PARAM = "timeAt"
const val ENDTIMEAT_PARAM = "endTimeAt"
const val AGGRPERIODDURATION_PARAM = "aggrPeriodDuration"
const val AGGRMETHODS_PARAM = "aggrMethods"
const val LASTN_PARAM = "lastN"
const val TIMEPROPERTY_PARAM = "timeproperty"

data class TemporalQuery(
    val timerel: Timerel? = null,
    val timeAt: ZonedDateTime? = null,
    val endTimeAt: ZonedDateTime? = null,
    val aggrPeriodDuration: String? = null,
    val aggrMethods: List<Aggregate>? = null,
    val hasLastN: Boolean,
    val instanceLimit: Int,
    val timeproperty: AttributeInstance.TemporalProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
) {

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
