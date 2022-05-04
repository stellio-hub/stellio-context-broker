package com.egm.stellio.search.model

import java.time.ZonedDateTime

data class TemporalQuery(
    val expandedAttrs: Set<String> = emptySet(),
    val timerel: Timerel? = null,
    val timeAt: ZonedDateTime? = null,
    val endTimeAt: ZonedDateTime? = null,
    val timeBucket: String? = null,
    val aggregate: Aggregate? = null,
    val lastN: Int? = null,
    val timeproperty: AttributeInstance.TemporalProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
) {
    enum class Timerel {
        BEFORE,
        AFTER,
        BETWEEN
    }

    enum class Aggregate {
        AVG,
        SUM,
        COUNT,
        MIN,
        MAX;

        companion object {
            fun isSupportedAggregate(aggregate: String): Boolean =
                values().toList().any { it.name == aggregate.uppercase() }
        }
    }
}
