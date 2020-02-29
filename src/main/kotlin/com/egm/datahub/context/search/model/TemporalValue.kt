package com.egm.datahub.context.search.model

import com.egm.datahub.context.search.util.TemporalValueSerializer
import com.fasterxml.jackson.databind.annotation.JsonSerialize

@JsonSerialize(using = TemporalValueSerializer::class)
data class TemporalValue(
    val value: Double,
    val timestamp: String
)