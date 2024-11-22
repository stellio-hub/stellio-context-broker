package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.param.GeoQuery
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.io.WKTWriter
import org.locationtech.jts.io.geojson.GeoJsonReader
import org.locationtech.jts.io.geojson.GeoJsonWriter

const val FEATURE_TYPE = "Feature"
const val FEATURE_COLLECTION_TYPE = "FeatureCollection"
const val GEOMETRY_PROPERTY_TERM = "geometry"
const val PROPERTIES_PROPERTY_TERM = "properties"
const val FEATURES_PROPERTY_TERM = "features"

fun geoJsonToWkt(geometryType: GeoQuery.GeometryType, coordinates: String): Either<APIException, WKTCoordinates> =
    geoJsonToWkt(
        """
        {
            "type": "${geometryType.type}",
            "coordinates": $coordinates
        }        
        """.trimIndent()
    ).map {
        WKTCoordinates(it)
    }

fun geoJsonToWkt(geoJsonPayload: Map<String, Any>): Either<APIException, String> =
    geoJsonToWkt(serializeObject(geoJsonPayload))

fun geoJsonToWkt(geoJsonSerializedPayload: String): Either<APIException, String> =
    runCatching {
        val geoJson = GeoJsonReader().read(geoJsonSerializedPayload)
        WKTWriter().write(geoJson)
    }.fold({
        it.right()
    }, {
        BadRequestDataException("Invalid geometry definition: $geoJsonSerializedPayload ($it)").left()
    })

fun throwingGeoJsonToWkt(geoJsonPayload: Map<String, Any>): String =
    geoJsonToWkt(geoJsonPayload).fold({ throw it }, { it })

fun wktToGeoJson(wkt: String): Map<String, Any> {
    val geometry = WKTReader().read(wkt)
    val geoJsonWriter = GeoJsonWriter()
    geoJsonWriter.setEncodeCRS(false)
    return deserializeObject(geoJsonWriter.write(geometry))
}
