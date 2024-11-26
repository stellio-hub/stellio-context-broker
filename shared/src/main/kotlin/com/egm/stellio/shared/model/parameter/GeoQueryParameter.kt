package com.egm.stellio.shared.model.parameter

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.decode
import com.egm.stellio.shared.util.geoJsonToWkt

enum class GeoQueryParameter(
    override val key: String,
    override val implemented: Boolean = true,
) : QueryParameter {
    GEOREL("georel"),
    GEOMETRY("geometry"),
    COORDINATES("coordinates"),
    GEOPROPERTY("geoproperty");
    enum class Georel(val key: String) {
        NEAR("near"),
        WITHIN("within"),
        CONTAINS("contains"),
        INTERSECTS("intersects"),
        EQUALS("equals"),
        DISJOINT("disjoint"),
        OVERLAPS("overlaps");
        companion object {
            val ALL = Georel.entries.map { it.key }

            const val NEAR_DISTANCE_MODIFIER = "distance"
            const val NEAR_MAXDISTANCE_MODIFIER = "maxDistance"
            private val nearRegex = "^near;(?:minDistance|maxDistance)==\\d+$".toRegex()

            fun verify(georel: String): Either<APIException, Unit> {
                if (georel.startsWith(GeoQueryParameter.Georel.NEAR.key)) {
                    if (!georel.matches(nearRegex))
                        return BadRequestDataException("Invalid expression for 'near' georel: $georel").left()
                    return Unit.right()
                } else if (GeoQueryParameter.Georel.ALL.any { georel == it })
                    return Unit.right()
                else return BadRequestDataException("Invalid 'georel' parameter provided: $georel").left()
            }

            fun prepareQuery(georel: String): Triple<String, String?, String?> =
                if (georel.startsWith(GeoQueryParameter.Georel.NEAR.key)) {
                    val comparisonParams = georel.split(";")[1].split("==")
                    if (comparisonParams[0] == NEAR_MAXDISTANCE_MODIFIER)
                        Triple(NEAR_DISTANCE_MODIFIER, "<=", comparisonParams[1])
                    else Triple(NEAR_DISTANCE_MODIFIER, ">=", comparisonParams[1])
                } else Triple(georel, null, null)
        }
    }

    companion object {

        fun parseGeoQueryParameters( // todo better name? "toGeoQuery"? or move it to GeoQueryFile?
            requestParams: Map<String, String>,
            contexts: List<String>
        ): Either<APIException, GeoQuery?> = either {
            val georel = requestParams[GeoQueryParameter.GEOREL.key]?.decode()?.also {
                Georel.verify(it).bind()
            }
            val geometry = requestParams[GeoQueryParameter.GEOMETRY.key]?.let {
                if (GeoQuery.GeometryType.isSupportedType(it))
                    GeoQuery.GeometryType.forType(it).right()
                else
                    BadRequestDataException("$it is not a recognized value for 'geometry' parameter").left()
            }?.bind()
            val coordinates = requestParams[GeoQueryParameter.COORDINATES.key]?.decode()?.let {
                stringifyCoordinates(it)
            }
            val geoproperty = requestParams[GeoQueryParameter.GEOPROPERTY.key]?.let {
                expandJsonLdTerm(it, contexts)
            } ?: JsonLdUtils.NGSILD_LOCATION_PROPERTY

            // if at least one parameter is provided, the three must be provided for the geoquery to be valid
            val notNullGeoParameters = listOfNotNull(georel, geometry, coordinates)
            if (notNullGeoParameters.isEmpty())
                null
            else if (georel == null || geometry == null || coordinates == null)
                BadRequestDataException(
                    "Missing at least one geo parameter between 'geometry', 'georel' and 'coordinates'"
                )
                    .left().bind<GeoQuery>()
            else
                GeoQuery(
                    georel = georel,
                    geometry = geometry,
                    coordinates = coordinates,
                    wktCoordinates = parseGeometryToWKT(geometry, coordinates).bind(),
                    geoproperty = geoproperty
                )
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
    }
}
