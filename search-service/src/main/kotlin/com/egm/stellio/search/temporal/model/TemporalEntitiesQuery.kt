package com.egm.stellio.search.temporal.model

import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.model.EntitiesQueryFromPost
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import java.time.Duration
import java.time.Period
import java.time.temporal.TemporalAmount

sealed class TemporalEntitiesQuery(
    open val entitiesQuery: EntitiesQuery,
    open val temporalQuery: TemporalQuery,
    open val temporalRepresentation: TemporalRepresentation,
    open val withAudit: Boolean
) {
    fun isAggregatedWithDefinedDuration(): Boolean =
        temporalRepresentation == TemporalRepresentation.AGGREGATED_VALUES &&
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
    override val temporalRepresentation: TemporalRepresentation,
    override val withAudit: Boolean
) : TemporalEntitiesQuery(entitiesQuery, temporalQuery, temporalRepresentation, withAudit)

data class TemporalEntitiesQueryFromPost(
    override val entitiesQuery: EntitiesQueryFromPost,
    override val temporalQuery: TemporalQuery,
    override val temporalRepresentation: TemporalRepresentation,
    override val withAudit: Boolean
) : TemporalEntitiesQuery(entitiesQuery, temporalQuery, temporalRepresentation, withAudit)
