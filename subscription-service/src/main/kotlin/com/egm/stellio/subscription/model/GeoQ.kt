package com.egm.stellio.subscription.model

import com.egm.stellio.shared.model.parameter.GeoQueryParameter
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
            GeoQueryParameter.GEOREL.key to georel,
            GeoQueryParameter.GEOMETRY.key to geometry,
            GeoQueryParameter.COORDINATES.key to coordinates,
            GeoQueryParameter.GEOPROPERTY.key to geoproperty
        )
}
