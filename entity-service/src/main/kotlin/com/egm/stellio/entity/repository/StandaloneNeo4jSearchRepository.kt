package com.egm.stellio.entity.repository

import arrow.core.Option
import com.egm.stellio.entity.model.GeoQuery
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.Sub
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.data.neo4j.core.Neo4jClient
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
class StandaloneNeo4jSearchRepository(
    private val neo4jClient: Neo4jClient
) : SearchRepository {

    override fun getEntities(
        queryParams: QueryParams,
        sub: Option<Sub>,
        geoQuery: GeoQuery,
        contexts: List<String>
    ): Pair<Int, List<URI>> {
        val query = QueryUtils.prepareQueryForEntitiesWithoutAuthentication(queryParams, geoQuery, contexts)
        val result = neo4jClient.query(query).fetch().all()
        return prepareResults(queryParams.limit, result)
    }
}
