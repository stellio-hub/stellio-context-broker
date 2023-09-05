package com.egm.stellio.search.model

import com.egm.stellio.search.scope.ScopeInstanceResult
import com.egm.stellio.search.util.TemporalEntityAttributeInstancesResult

data class EntityTemporalResult(
    val entityPayload: EntityPayload,
    val scopeHistory: List<ScopeInstanceResult>,
    val teaInstancesResult: TemporalEntityAttributeInstancesResult
)
