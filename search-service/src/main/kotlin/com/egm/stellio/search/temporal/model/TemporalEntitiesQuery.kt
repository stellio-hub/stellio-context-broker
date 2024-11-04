package com.egm.stellio.search.temporal.model

import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.model.EntitiesQueryFromPost
import java.time.Duration
import java.time.Period
import java.time.temporal.TemporalAmount

sealed class TemporalEntitiesQuery(
    open val entitiesQuery: EntitiesQuery,
    open val temporalQuery: TemporalQuery,
    open val withTemporalValues: Boolean,
    open val withAudit: Boolean,
    open val withAggregatedValues: Boolean
) {
    fun isAggregatedWithDefinedDuration(): Boolean =
        withAggregatedValues &&
            temporalQuery.aggrPeriodDuration != null &&
            temporalQuery.aggrPeriodDuration != "PT0S"

    fun computeAggrPeriodDuration(): TemporalAmount =
        temporalQuery.aggrPeriodDuration?.let { aggrPeriodDuration ->
            val splitted = aggrPeriodDuration.split("T")
            if (splitted.size == 1) // has only date-based fields
                Period.parse(aggrPeriodDuration)
            else {
                val duration = Duration.parse("PT" + splitted[1])
                if ("P" == splitted[0]) // has only time-based fields
                    duration
                else Period.parse(splitted[0]).plus(duration)
            }
        } ?: Period.ZERO
}

data class TemporalEntitiesQueryFromGet(
    override val entitiesQuery: EntitiesQueryFromGet,
    override val temporalQuery: TemporalQuery,
    override val withTemporalValues: Boolean,
    override val withAudit: Boolean,
    override val withAggregatedValues: Boolean
) : TemporalEntitiesQuery(entitiesQuery, temporalQuery, withTemporalValues, withAudit, withAggregatedValues)

data class TemporalEntitiesQueryFromPost(
    override val entitiesQuery: EntitiesQueryFromPost,
    override val temporalQuery: TemporalQuery,
    override val withTemporalValues: Boolean,
    override val withAudit: Boolean,
    override val withAggregatedValues: Boolean
) : TemporalEntitiesQuery(entitiesQuery, temporalQuery, withTemporalValues, withAudit, withAggregatedValues)
