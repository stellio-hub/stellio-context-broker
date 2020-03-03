package com.egm.datahub.context.subscription.model

data class GeoQuery(
    val georel: String,
    val geometry: GeometryType,
    val coordinates: String,
    val geoproperty: String? = null
) {
    enum class GeometryType(val geometry: String) {
        Point("Point"),
        Polygon("Polygon")
    }
}
