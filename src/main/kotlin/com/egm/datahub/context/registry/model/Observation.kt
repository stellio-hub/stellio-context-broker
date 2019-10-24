package com.egm.datahub.context.registry.model

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.OffsetDateTime

data class Observation(
    val id: String,
    val type: String,
    val observedBy: ObservedBy,
    val location: GeoProperty,
    val unitCode: String,
    val value: Double,
    val observedAt: OffsetDateTime
)

data class ObservedBy(
    val type: String,
    @JsonProperty("object") val target: String
)

data class GeoProperty(
    val type: String,
    val value: Value
)

data class Value(
    val type: String,
    val coordinates: List<Double>
)
