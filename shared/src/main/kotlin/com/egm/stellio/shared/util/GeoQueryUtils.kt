package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm

const val GEO_QUERY_PARAM_GEOREL = "georel"
const val GEO_QUERY_PARAM_GEOMETRY = "geometry"
const val GEO_QUERY_PARAM_COORDINATES = "coordinates"
const val GEO_QUERY_PARAM_GEOPROPERTY = "geoproperty"

const val GEO_QUERY_GEOREL_NEAR = "near"
const val GEO_QUERY_GEOREL_WITHIN = "within"
const val GEO_QUERY_GEOREL_CONTAINS = "contains"
const val GEO_QUERY_GEOREL_INTERSECTS = "intersects"
const val GEO_QUERY_GEOREL_EQUALS = "equals"
const val GEO_QUERY_GEOREL_DISJOINT = "disjoint"
const val GEO_QUERY_GEOREL_OVERLAPS = "overlaps"
val GEO_QUERY_ALL_GEORELS = listOf(
    GEO_QUERY_GEOREL_NEAR,
    GEO_QUERY_GEOREL_WITHIN,
    GEO_QUERY_GEOREL_CONTAINS,
    GEO_QUERY_GEOREL_INTERSECTS,
    GEO_QUERY_GEOREL_EQUALS,
    GEO_QUERY_GEOREL_DISJOINT,
    GEO_QUERY_GEOREL_OVERLAPS
)
const val GEOREL_NEAR_DISTANCE_MODIFIER = "distance"
const val GEOREL_NEAR_MAXDISTANCE_MODIFIER = "maxDistance"

private val georelNearRegex = "^near;(?:minDistance|maxDistance)==\\d+$".toRegex()

suspend fun parseGeoQueryParameters(
    requestParams: Map<String, String>,
    contextLink: String
): Either<APIException, GeoQuery?> = parseGeoQueryParameters(requestParams, listOf(contextLink))

suspend fun parseGeoQueryParameters(
    requestParams: Map<String, String>,
    contexts: List<String>
): Either<APIException, GeoQuery?> = either {
    val georel = requestParams[GEO_QUERY_PARAM_GEOREL]?.also {
        checkGeorelParam(it).bind()
    }
    val geometry = requestParams[GEO_QUERY_PARAM_GEOMETRY]?.let {
        if (GeoQuery.GeometryType.isSupportedType(it))
            GeoQuery.GeometryType.forType(it).right()
        else
            BadRequestDataException("$it is not a recognized value for 'geometry' parameter").left()
    }?.bind()
    val coordinates = requestParams[GEO_QUERY_PARAM_COORDINATES]?.decode()?.let {
        stringifyCoordinates(it)
    }
    val geoproperty = requestParams[GEO_QUERY_PARAM_GEOPROPERTY]?.let {
        expandJsonLdTerm(it, contexts)
    } ?: JsonLdUtils.NGSILD_LOCATION_PROPERTY

    // if at least one parameter is provided, the three must be provided for the geoquery to be valid
    val notNullGeoParameters = listOfNotNull(georel, geometry, coordinates)
    if (notNullGeoParameters.isEmpty())
        null
    else if (georel == null || geometry == null || coordinates == null)
        BadRequestDataException("Missing at least one geo parameter between 'geometry', 'georel' and 'coordinates'")
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

fun checkGeorelParam(georel: String): Either<APIException, Unit> {
    if (georel.startsWith(GEO_QUERY_GEOREL_NEAR)) {
        if (!georel.matches(georelNearRegex))
            return BadRequestDataException("Invalid expression for 'near' georel: $georel").left()
        return Unit.right()
    } else if (GEO_QUERY_ALL_GEORELS.any { georel == it })
        return Unit.right()
    else return BadRequestDataException("Invalid 'georel' parameter provided: $georel").left()
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

private fun prepareGeorelQuery(georel: String): Triple<String, String?, String?> =
    if (georel.startsWith(GEO_QUERY_GEOREL_NEAR)) {
        val comparisonParams = georel.split(";")[1].split("==")
        when (comparisonParams[0]) {
            GEOREL_NEAR_MAXDISTANCE_MODIFIER -> Triple(GEOREL_NEAR_DISTANCE_MODIFIER, "<=", comparisonParams[1])
            else -> Triple(GEOREL_NEAR_DISTANCE_MODIFIER, ">=", comparisonParams[1])
        }
    } else Triple(georel, null, null)

fun buildGeoQuery(geoQuery: GeoQuery, target: JsonLdEntity? = null): String {
    val targetWKTCoordinates =
        """
        (select jsonb_path_query_first(#{TARGET}#, '$."${geoQuery.geoproperty}"."$NGSILD_GEOPROPERTY_VALUE"[0]')->>'$JSONLD_VALUE')
        """.trimIndent()
    val georelQuery = prepareGeorelQuery(geoQuery.georel)

    return (
        if (georelQuery.first == GEOREL_NEAR_DISTANCE_MODIFIER)
            """
            public.ST_Distance(
                'SRID=4326;${geoQuery.wktCoordinates.value}'::geography, 
                ('SRID=4326;' || $targetWKTCoordinates)::geography,
                false
            ) ${georelQuery.second} ${georelQuery.third}
            """.trimIndent()
        else
            """
            public.ST_${georelQuery.first}(
                public.ST_GeomFromText('${geoQuery.wktCoordinates.value}'), 
                public.ST_GeomFromText($targetWKTCoordinates)
            ) 
            """.trimIndent()
        )
        .let {
            if (target == null)
                it.replace("#{TARGET}#", "entity_payload.payload")
            else
                it.replace("#{TARGET}#", "'" + JsonUtils.serializeObject(target.members) + "'")
        }
}
