package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NgsiLdGeoProperty
import com.egm.stellio.shared.util.buildQQuery
import com.egm.stellio.shared.util.buildTypeQuery
import com.egm.stellio.subscription.model.GeoQuery

object QueryUtils {

    private const val NEAR_QUERY_CLAUSE = "near"
    private const val DISTANCE_QUERY_CLAUSE = "distance"
    private const val MAX_DISTANCE_QUERY_CLAUSE = "maxDistance"
    private const val MIN_DISTANCE_QUERY_CLAUSE = "minDistance"

    fun createTypeStatement(typesQuery: List<String>, types: List<ExpandedTerm>): String {
        val filterTypesQuery = typesQuery.joinToString(" OR ") { buildTypeQuery(it, types) }
        return """
        SELECT $filterTypesQuery AS match
        """.trimIndent()
    }

    fun createQueryStatement(query: String, jsonLdEntity: JsonLdEntity, contexts: List<String>): String {
        val filterQuery = buildQQuery(query, contexts, jsonLdEntity)
        return """
        SELECT $filterQuery AS match
        """.trimIndent()
    }

    fun createGeoQueryStatement(geoQuery: GeoQuery?, geoProperty: NgsiLdGeoProperty): String {
        val targetWKTCoordinates = geoProperty.instances[0].coordinates.value
        val georelParams = extractGeorelParams(geoQuery!!.georel)

        return if (georelParams.first == DISTANCE_QUERY_CLAUSE)
            """
            SELECT ST_Distance('${geoQuery.pgisGeometry}'::geography, 
                'SRID=4326;$targetWKTCoordinates'::geography) ${georelParams.second} ${georelParams.third} as match
            """.trimIndent()
        else
            """
            SELECT ST_${georelParams.first}('${geoQuery.pgisGeometry}', ST_GeomFromText('$targetWKTCoordinates')) 
                as match
            """.trimIndent()
    }

    private fun extractGeorelParams(georel: String): Triple<String, String?, String?> {
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
}
