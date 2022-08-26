package com.egm.stellio.entity.repository

import arrow.core.Option
import com.egm.stellio.entity.authorization.Neo4jAuthorizationService
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.GeoQueryUtils.MULTIPLY_DISTANCE
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.operation.distance.DistanceOp.distance
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.data.neo4j.core.Neo4jClient
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled")
class Neo4jSearchRepository(
    private val neo4jAuthorizationService: Neo4jAuthorizationService,
    private val neo4jClient: Neo4jClient
) : SearchRepository {

    override fun getEntities(
        queryParams: QueryParams,
        sub: Option<Sub>,
        contexts: List<String>
    ): Pair<Int, List<URI>> {
        val userAndGroupIds = neo4jAuthorizationService.getSubjectGroups(sub)
            .plus(neo4jAuthorizationService.getSubjectUri(sub))
            .map { it.toString() }

        val query = if (neo4jAuthorizationService.userIsAdmin(sub))
            QueryUtils.prepareQueryForEntitiesWithoutAuthentication(queryParams, queryParams.geoQuery, contexts)
        else
            QueryUtils.prepareQueryForEntitiesWithAuthentication(queryParams, queryParams.geoQuery, contexts)

        val result = neo4jClient
            .query(query)
            .bind(userAndGroupIds).to("userAndGroupIds")
            .fetch()
            .all()

        if (verifGeoQuery(queryParams.geoQuery)) {
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
                val distance = distance(geo1, geo2) * MULTIPLY_DISTANCE
                if (georelParams.second.equals("<=")) {
                    if (distance <= georelParams.third!!.toDouble()) geoResult.add(it)
                } else if (georelParams.second.equals(">=")) {
                    if (distance >= georelParams.third!!.toDouble()) geoResult.add(it)
                } else if (georelParams.second.equals("==")) {
                    if (distance(geo1, geo2) != georelParams.third!!.toDouble()) geoResult.add(it)
                }
            }
            return return prepareResults(queryParams.limit, geoResult)
        } else {
            return prepareResults(queryParams.limit, result)
        }
    }
}
