package com.egm.datahub.context.search.model

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.OffsetDateTime

data class NgsiLdObservation(
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
