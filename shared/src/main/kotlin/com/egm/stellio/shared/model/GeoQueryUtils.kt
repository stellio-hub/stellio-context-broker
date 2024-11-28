package com.egm.stellio.shared.model

import arrow.core.Either
import com.egm.stellio.shared.model.parameter.GeoQuery
import com.egm.stellio.shared.util.geoJsonToWkt

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
