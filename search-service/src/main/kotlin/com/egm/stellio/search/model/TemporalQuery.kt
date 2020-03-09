package com.egm.stellio.search.model

import java.time.OffsetDateTime

data class TemporalQuery(
    val attrs: List<String> = emptyList(),
    val timerel: Timerel,
    val time: OffsetDateTime,
    val endTime: OffsetDateTime?
) {
    enum class Timerel {
        BEFORE,
        AFTER,
        BETWEEN
    }
}
