package com.egm.stellio.search.model

import java.time.ZonedDateTime

data class TemporalQuery(
    val attrs: Set<String> = emptySet(),
    val timerel: Timerel? = null,
    val time: ZonedDateTime? = null,
    val endTime: ZonedDateTime? = null,
    val timeBucket: String? = null,
    val aggregate: Aggregate? = null,
    val lastN: Int? = null
) {
    enum class Timerel {
        BEFORE,
        AFTER,
        BETWEEN
    }

    enum class Aggregate {
        AVG,
        SUM;

        companion object {
            fun isSupportedAggregate(aggregate: String): Boolean =
                values().toList().any { it.name == aggregate.toUpperCase() }
        }
    }
}
