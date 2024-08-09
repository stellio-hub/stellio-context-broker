package com.egm.stellio.search.temporal.model

import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.scope.ScopeInstanceResult
import com.egm.stellio.search.temporal.util.AttributesWithInstances

data class EntityTemporalResult(
    val entity: Entity,
    val scopeHistory: List<ScopeInstanceResult>,
    val attributesWithInstances: AttributesWithInstances
)
