package com.egm.stellio.search.model

import com.egm.stellio.search.util.RawValueSerializer
import com.fasterxml.jackson.databind.annotation.JsonSerialize

@JsonSerialize(using = RawValueSerializer::class)
data class RawValue(
    val value: Any,
    val timestamp: String
)
