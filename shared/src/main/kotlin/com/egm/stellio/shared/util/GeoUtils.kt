package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.io.WKTWriter
import org.locationtech.jts.io.geojson.GeoJsonReader
import org.locationtech.jts.io.geojson.GeoJsonWriter

fun geoJsonToWkt(geometryType: String, coordinates: String): String =
    geoJsonToWkt(
        """
        {
            "type": "$geometryType",
            "coordinates": $coordinates
        }        
        """.trimIndent()
    )

fun geoJsonToWkt(geoJsonPayload: Map<String, Any>): String =
    geoJsonToWkt(serializeObject(geoJsonPayload))

fun geoJsonToWkt(geoJsonSerializedPayload: String): String {
    val geoJson = GeoJsonReader().read(geoJsonSerializedPayload)
    return WKTWriter().write(geoJson)
}

fun wktToGeoJson(wkt: String): Map<String, Any> {
    val geometry = WKTReader().read(wkt)
    val geoJsonWriter = GeoJsonWriter()
    geoJsonWriter.setEncodeCRS(false)
    return deserializeObject(geoJsonWriter.write(geometry))
}
