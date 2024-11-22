package com.egm.stellio.shared.model.param

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.WKTCoordinates
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
                entries.any { it.type == type }

            fun forType(type: String): GeometryType? =
                entries.find { it.type == type }
        }
    }
}
