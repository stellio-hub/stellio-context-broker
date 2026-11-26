package com.egm.stellio.search.csr.model

import com.egm.stellio.search.temporal.model.TemporalPagination

data class DistributedTemporalQueryResult<out T>(
    val warnings: List<NGSILDWarning> = emptyList(),
    val entities: List<T> = emptyList(),
    val count: Int = 0,
    val pagination: TemporalPagination = TemporalPagination()
)
