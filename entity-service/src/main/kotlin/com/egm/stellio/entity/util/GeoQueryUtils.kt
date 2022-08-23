package com.egm.stellio.entity.util

import com.egm.stellio.entity.model.GeoQuery
import com.egm.stellio.shared.util.GeoQueryUtils.DISTANCE_QUERY_CLAUSE
import com.egm.stellio.shared.util.GeoQueryUtils.GEO_QUERY_PARAM_COORDINATES
import com.egm.stellio.shared.util.GeoQueryUtils.GEO_QUERY_PARAM_GEOMETRY
import com.egm.stellio.shared.util.GeoQueryUtils.GEO_QUERY_PARAM_GEOPROPERTY
import com.egm.stellio.shared.util.GeoQueryUtils.GEO_QUERY_PARAM_GEOREL
import com.egm.stellio.shared.util.GeoQueryUtils.MAX_DISTANCE_QUERY_CLAUSE
import com.egm.stellio.shared.util.GeoQueryUtils.MIN_DISTANCE_QUERY_CLAUSE
import com.egm.stellio.shared.util.GeoQueryUtils.NEAR_QUERY_CLAUSE
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import org.springframework.util.MultiValueMap

const val MULTIPLY_DISTANCE = 1000

fun parseAndCheckGeoQuery(requestParams: MultiValueMap<String, String>, contextLink: String): GeoQuery {

    val georel = requestParams.getFirst(GEO_QUERY_PARAM_GEOREL)
    val geometry = requestParams.getFirst(GEO_QUERY_PARAM_GEOMETRY)
    val coordinates = requestParams.getFirst(GEO_QUERY_PARAM_COORDINATES)
    val geoproperty = requestParams.getFirst(GEO_QUERY_PARAM_GEOPROPERTY)?.let { expandJsonLdTerm(it, contextLink) }

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
            else -> Triple(DISTANCE_QUERY_CLAUSE, "==", comparisonParams[1])
        }
    }
    return Triple(georel, null, null)
}
