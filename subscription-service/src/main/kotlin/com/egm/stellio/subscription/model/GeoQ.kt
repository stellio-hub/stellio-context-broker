package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.GEO_QUERY_PARAM_COORDINATES
import com.egm.stellio.shared.util.GEO_QUERY_PARAM_GEOMETRY
import com.egm.stellio.shared.util.GEO_QUERY_PARAM_GEOPROPERTY
import com.egm.stellio.shared.util.GEO_QUERY_PARAM_GEOREL
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
            GEO_QUERY_PARAM_GEOREL to georel,
            GEO_QUERY_PARAM_GEOMETRY to geometry,
            GEO_QUERY_PARAM_COORDINATES to coordinates,
            GEO_QUERY_PARAM_GEOPROPERTY to geoproperty
        )
}
