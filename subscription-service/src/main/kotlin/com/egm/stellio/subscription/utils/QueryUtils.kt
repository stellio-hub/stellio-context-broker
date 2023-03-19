package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.model.GeoQuery
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.util.buildGeoQuery
import com.egm.stellio.shared.util.buildQQuery
import com.egm.stellio.subscription.model.GeoQ

object QueryUtils {

    fun createQueryStatement(query: String, jsonLdEntity: JsonLdEntity, contexts: List<String>): String {
        val filterQuery = buildQQuery(query, contexts, jsonLdEntity)
        return """
        SELECT $filterQuery AS match
        """.trimIndent()
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

        return """
        SELECT $sqlGeoQuery as match
        """.trimIndent()
    }
}
