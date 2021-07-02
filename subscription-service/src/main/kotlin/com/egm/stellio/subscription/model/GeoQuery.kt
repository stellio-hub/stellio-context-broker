package com.egm.stellio.subscription.model

import org.springframework.data.relational.core.mapping.Table

@Table(value = "geometry_query")
data class GeoQuery(
    val georel: String,
    val geometry: GeometryType,
    val coordinates: Any,
    val geoproperty: String? = null
) {
    enum class GeometryType(val geometry: String) {
        Point("Point"),
        Polygon("Polygon")
    }
}
