package com.egm.stellio.subscription.model

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
