package com.egm.stellio.search.model

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
}
