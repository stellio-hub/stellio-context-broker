package com.egm.stellio.search.model

import com.egm.stellio.search.util.TemporalValueSerializer
import com.fasterxml.jackson.databind.annotation.JsonSerialize

@JsonSerialize(using = TemporalValueSerializer::class)
data class TemporalValue(
    val value: Double,
    val timestamp: String
)