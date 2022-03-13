package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.JsonUtils.serializeObject
import org.locationtech.jts.io.WKTWriter
import org.locationtech.jts.io.geojson.GeoJsonReader

fun geoJsonToWKT(geometryType: String, coordinates: String): String =
    geoJsonToWKT(
        """
        {
            "type": "$geometryType",
            "coordinates": $coordinates
        }        
        """.trimIndent()
    )

fun geoJsonToWKT(geoJsonPayload: Map<String, Any>): String =
    geoJsonToWKT(serializeObject(geoJsonPayload))

fun geoJsonToWKT(geoJsonSerializedPayload: String): String {
    val geoJson = GeoJsonReader().read(geoJsonSerializedPayload)
    return WKTWriter().write(geoJson)
}
