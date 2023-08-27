package com.egm.stellio.search.model

import com.egm.stellio.search.service.ScopeService
import com.egm.stellio.search.util.TemporalEntityAttributeInstancesResult

data class EntityTemporalResult(
    val entityPayload: EntityPayload,
    val scopeHistory: List<ScopeService.ScopeHistoryEntry>,
    val teaInstancesResult: TemporalEntityAttributeInstancesResult
)
