package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.model.GeoQuery
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.util.*
import com.egm.stellio.subscription.model.GeoQ

object QueryUtils {

    fun createTypeStatement(typesQuery: List<String>, types: List<ExpandedTerm>): String {
        val filterTypesQuery = typesQuery.joinToString(" OR ") { buildTypeQuery(it, types) }
        return "SELECT $filterTypesQuery AS match"
    }

    fun createQueryStatement(query: String, jsonLdEntity: JsonLdEntity, contexts: List<String>): String {
        val filterQuery = buildQQuery(query, contexts, jsonLdEntity)
        return "SELECT $filterQuery AS match"
    }

    fun createScopeQueryStatement(scopeQ: String, jsonLdEntity: JsonLdEntity): String {
        val filterQuery = buildScopeQQuery(scopeQ, jsonLdEntity)
        return "SELECT $filterQuery AS match"
    }

    fun createGeoQueryStatement(geoQ: GeoQ, jsonLdEntity: JsonLdEntity): String {
        val sqlGeoQuery = buildGeoQuery(
            GeoQuery(
                georel = geoQ.georel,
                geometry = GeoQuery.GeometryType.forType(geoQ.geometry)!!,
                coordinates = geoQ.coordinates,
                geoproperty = geoQ.geoproperty,
                wktCoordinates = WKTCoordinates(geoQ.pgisGeometry!!)
            ),
            jsonLdEntity
        )

        return "SELECT $sqlGeoQuery as match"
    }
}
