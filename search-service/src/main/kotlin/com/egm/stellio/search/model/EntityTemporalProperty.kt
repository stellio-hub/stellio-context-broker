package com.egm.stellio.search.model

data class EntityTemporalProperty(
    val entityId: String,
    val type: String,
    val attributeName: String,
    val observedBy: String
)