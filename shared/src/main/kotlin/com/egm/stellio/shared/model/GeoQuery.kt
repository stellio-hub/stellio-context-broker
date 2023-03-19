package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY

data class GeoQuery(
    val georel: String,
    val geometry: GeometryType,
    val coordinates: String,
    val wktCoordinates: WKTCoordinates,
    var geoproperty: ExpandedTerm = NGSILD_LOCATION_PROPERTY
) {
    enum class GeometryType(val type: String) {
        POINT("Point"),
        MULTIPOINT("MultiPoint"),
        LINESTRING("LineString"),
        MULTILINESTRING("MultiLineString"),
        POLYGON("Polygon"),
        MULTIPOLYGON("MultiPolygon");

        companion object {
            fun isSupportedType(type: String): Boolean =
                values().any { it.type == type }

            fun forType(type: String): GeometryType? =
                values().find { it.type == type }
        }
    }
}
