package com.egm.stellio.search.temporal.model

import com.egm.stellio.search.entity.model.EntityPayload
import com.egm.stellio.search.scope.ScopeInstanceResult
import com.egm.stellio.search.temporal.util.TemporalEntityAttributeInstancesResult

data class EntityTemporalResult(
    val entityPayload: EntityPayload,
    val scopeHistory: List<ScopeInstanceResult>,
    val teaInstancesResult: TemporalEntityAttributeInstancesResult
)
