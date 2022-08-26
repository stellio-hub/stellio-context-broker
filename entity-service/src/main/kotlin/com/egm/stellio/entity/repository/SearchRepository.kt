package com.egm.stellio.entity.repository

import arrow.core.Option
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.*
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.operation.distance.DistanceOp
import org.springframework.transaction.annotation.Transactional
import java.net.URI

interface SearchRepository {

    /**
     * Searches the requested entities and applies permissions checks in authentication enabled mode.
     *
     * @param queryParams query parameters.
     * @param sub to be used in authentication enabled mode to apply permissions checks.
     * @param offset offset for pagination.
     * @param limit limit for pagination.
     * @param contexts list of JSON-LD contexts for term to URI expansion.
     * @return [Pair]
     *  @property first count of all matching entities in the database.
     *  @property second list of matching entities ids as requested by pagination sorted by entity id.
     */
    @Transactional(readOnly = true)
    fun getEntities(
        queryParams: QueryParams,
        sub: Option<Sub>,
        contexts: List<String>
    ): Pair<Int, List<URI>>

    fun prepareResults(
        limit: Int,
        result: Collection<Map<String, Any>>,
        queryParams: QueryParams
    ): Pair<Int, List<URI>> =
        if (limit == 0)
            Pair(
                (result.firstOrNull()?.get("count") as? Long)?.toInt() ?: 0,
                emptyList()
            )
        else if (verifGeoQuery(queryParams.geoQuery)) {
            prepareResultsFilterByGeoQuery(queryParams.limit, result, queryParams)
        } else Pair(
            (result.firstOrNull()?.get("count") as? Long)?.toInt() ?: 0,
            result.map { (it["id"] as String).toUri() }
        )

    fun prepareResultsFilterByGeoQuery(
        limit: Int,
        result: Collection<Map<String, Any>>,
        queryParams: QueryParams
    ): Pair<Int, List<URI>> {
        val geoResult: ArrayList<Map<String, Any>> = ArrayList()
        val geo1 = WKTReader().read(
            geoJsonToWkt(
                queryParams.geoQuery.geometry!!,
                queryParams.geoQuery.coordinates.toString()
            )
        )
        val georelParams = extractGeorelParams(queryParams.geoQuery.georel!!)

        result.forEach {
            val geo2 = WKTReader().read(it["entityLocation"] as String)
            val distance = DistanceOp.distance(geo1, geo2) * GeoQueryUtils.MULTIPLY_DISTANCE
            if (georelParams.second.equals("<=")) {
                if (distance <= georelParams.third!!.toDouble()) geoResult.add(it)
            } else if (georelParams.second.equals(">=")) {
                if (distance >= georelParams.third!!.toDouble()) geoResult.add(it)
            } else {
                if (distance != georelParams.third!!.toDouble()) geoResult.add(it)
            }
        }
        return Pair(
            (geoResult.firstOrNull()?.get("count") as? Long)?.toInt() ?: 0,
            geoResult.map { (it["id"] as String).toUri() }
        )
    }
}
