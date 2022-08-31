package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.GeoQuery
import com.egm.stellio.shared.util.GeoQueryUtils.DISTANCE_QUERY_CLAUSE
import com.egm.stellio.shared.util.GeoQueryUtils.MAX_DISTANCE_QUERY_CLAUSE
import com.egm.stellio.shared.util.GeoQueryUtils.MIN_DISTANCE_QUERY_CLAUSE
import com.egm.stellio.shared.util.GeoQueryUtils.NEAR_QUERY_CLAUSE
import org.springframework.util.MultiValueMap

object GeoQueryUtils {
    const val GEO_QUERY_PARAM_GEOREL = "georel"
    const val GEO_QUERY_PARAM_GEOMETRY = "geometry"
    const val GEO_QUERY_PARAM_COORDINATES = "coordinates"
    const val GEO_QUERY_PARAM_GEOPROPERTY = "geoproperty"
    const val NEAR_QUERY_CLAUSE = "near"
    const val DISTANCE_QUERY_CLAUSE = "distance"
    const val MAX_DISTANCE_QUERY_CLAUSE = "maxDistance"
    const val MIN_DISTANCE_QUERY_CLAUSE = "minDistance"
}

fun parseAndCheckGeoQuery(requestParams: MultiValueMap<String, String>, contextLink: String): GeoQuery {
    val georel = requestParams.getFirst(GeoQueryUtils.GEO_QUERY_PARAM_GEOREL)
    val geometry = requestParams.getFirst(GeoQueryUtils.GEO_QUERY_PARAM_GEOMETRY)
    val coordinates = requestParams.getFirst(GeoQueryUtils.GEO_QUERY_PARAM_COORDINATES)?.decode()
    val geoproperty = requestParams.getFirst(GeoQueryUtils.GEO_QUERY_PARAM_GEOPROPERTY)?.let {
        JsonLdUtils.expandJsonLdTerm(it, contextLink)
    } ?: JsonLdUtils.NGSILD_LOCATION_PROPERTY

    return GeoQuery(
        georel = georel,
        geometry = geometry,
        coordinates = coordinates,
        geoproperty = geoproperty
    )
}

fun extractGeorelParams(georel: String): Triple<String, String?, String?> {
    if (georel.contains(NEAR_QUERY_CLAUSE)) {
        val comparisonParams = georel.split(";")[1].split("==")
        return when (comparisonParams[0]) {
            MAX_DISTANCE_QUERY_CLAUSE -> Triple(DISTANCE_QUERY_CLAUSE, "<=", comparisonParams[1])
            MIN_DISTANCE_QUERY_CLAUSE -> Triple(DISTANCE_QUERY_CLAUSE, ">=", comparisonParams[1])
            // defaulting to an equality, maybe we should raise a 400 at creation time?
            else -> Triple(DISTANCE_QUERY_CLAUSE, "=", comparisonParams[1])
        }
    }
    return Triple(georel, null, null)
}

fun isSupportedGeoQuery(geoQuery: GeoQuery): Boolean {
    return geoQuery.geoproperty == JsonLdUtils.NGSILD_LOCATION_PROPERTY &&
        geoQuery.georel != null &&
        geoQuery.geometry == "Point" &&
        geoQuery.coordinates != null
}
