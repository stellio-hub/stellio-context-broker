package com.egm.stellio.search.model

import java.net.URI

data class TemporalEntitiesQuery(
    val ids: Set<URI>,
    val types: Set<String>,
    val temporalQuery: TemporalQuery,
    val withTemporalValues: Boolean,
    val limit: Int,
    val offset: Int
)
