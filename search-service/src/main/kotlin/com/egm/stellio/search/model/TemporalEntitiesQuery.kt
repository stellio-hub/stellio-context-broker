package com.egm.stellio.search.model

import com.egm.stellio.shared.model.QueryParams

data class TemporalEntitiesQuery(
    val queryParams: QueryParams,
    val temporalQuery: TemporalQuery,
    val withTemporalValues: Boolean,
    val withAudit: Boolean,
    val withAggregatedValues: Boolean
)
