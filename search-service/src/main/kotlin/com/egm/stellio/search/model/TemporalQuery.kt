package com.egm.stellio.search.model

import java.time.ZonedDateTime

data class TemporalQuery(
    val attrs: List<String> = emptyList(),
    val timerel: Timerel,
    val time: ZonedDateTime,
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
