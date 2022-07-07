package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.fasterxml.jackson.annotation.JsonIgnore
import org.springframework.data.relational.core.mapping.Table

@Table(value = "geometry_query")
data class GeoQuery(
    val georel: String,
    val geometry: String,
    val coordinates: Any,
    @JsonIgnore
    val pgisGeometry: String?,
    var geoproperty: String? = NGSILD_LOCATION_PROPERTY
)
