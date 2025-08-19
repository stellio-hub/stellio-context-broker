package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NGSILD_NULL
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.queryparameter.GeoQuery
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

fun wktToGeoJson(wkt: String): Any =
    // when rendering null values in notifications, there is the special NGSI-LD Null instead of a WKT
    if (wkt == NGSILD_NULL)
        NGSILD_NULL
    else {
        val geometry = WKTReader().read(wkt)
        val geoJsonWriter = GeoJsonWriter()
        geoJsonWriter.setEncodeCRS(false)
        deserializeObject(geoJsonWriter.write(geometry))
    }

fun stringifyCoordinates(coordinates: Any): String =
    when (coordinates) {
        is String -> coordinates
        is List<*> -> coordinates.toString()
        else -> coordinates.toString()
    }

fun parseGeometryToWKT(
    geometryType: GeoQuery.GeometryType,
    coordinates: String
): Either<APIException, WKTCoordinates> =
    geoJsonToWkt(geometryType, coordinates)
