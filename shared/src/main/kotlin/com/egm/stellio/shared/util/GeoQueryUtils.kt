package com.egm.stellio.shared.util

import arrow.core.Either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.model.parameter.GeoQuery

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
