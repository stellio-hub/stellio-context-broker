package com.egm.stellio.search.temporal.model

import com.egm.stellio.search.entity.model.EntitiesQuery
import java.time.Duration
import java.time.Period
import java.time.temporal.TemporalAmount

data class TemporalEntitiesQuery(
    val entitiesQuery: EntitiesQuery,
    val temporalQuery: TemporalQuery,
    val withTemporalValues: Boolean,
    val withAudit: Boolean,
    val withAggregatedValues: Boolean
) {
    fun isAggregatedWithDefinedDuration(): Boolean =
        withAggregatedValues &&
            (temporalQuery.aggrPeriodDuration != null && temporalQuery.aggrPeriodDuration != "PT0S")

    fun computeAggrPeriodDuration(): TemporalAmount {
        val splitted = temporalQuery.aggrPeriodDuration!!.split("T")
        return if (splitted.size == 1) // has only date-based fields
            Period.parse(temporalQuery.aggrPeriodDuration)
        else {
            val duration = Duration.parse("PT" + splitted[1])
            if ("P" == splitted[0]) // has only time-based fields
                duration
            else Period.parse(splitted[0]).plus(duration)
        }
    }
}
