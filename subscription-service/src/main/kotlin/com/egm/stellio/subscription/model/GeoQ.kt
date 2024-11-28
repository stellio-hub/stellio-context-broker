package com.egm.stellio.subscription.model

import com.egm.stellio.shared.model.parameter.QueryParameter
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.fasterxml.jackson.annotation.JsonIgnore
import org.springframework.data.relational.core.mapping.Table

@Table(value = "geometry_query")
data class GeoQ(
    val georel: String,
    val geometry: String,
    val coordinates: String,
    @JsonIgnore
    val pgisGeometry: String?,
    val geoproperty: String = NGSILD_LOCATION_PROPERTY
) {
    // representation passed to function checking for the correctness of geo-queries
    fun toMap(): Map<String, String> =
        mapOf(
            QueryParameter.GEOREL.key to georel,
            QueryParameter.GEOMETRY.key to geometry,
            QueryParameter.COORDINATES.key to coordinates,
            QueryParameter.GEOPROPERTY.key to geoproperty
        )
}
