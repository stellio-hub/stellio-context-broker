package com.egm.stellio.subscription.model

import com.fasterxml.jackson.annotation.JsonIgnore
import org.springframework.data.relational.core.mapping.Table

@Table(value = "geometry_query")
data class GeoQuery(
    val georel: String,
    val geometry: String,
    val coordinates: Any,
    @JsonIgnore
    val pgisGeometry: String?,
    val geoproperty: String? = null
)
