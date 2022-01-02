package com.egm.stellio.entity.repository

import com.egm.stellio.entity.authorization.Neo4jAuthorizationService
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.AuthContextModel.USER_PREFIX
import com.egm.stellio.shared.util.toUri
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
        userSub: String,
        offset: Int,
        limit: Int,
        contexts: List<String>
    ): Pair<Int, List<URI>> {
        val query = if (neo4jAuthorizationService.userIsAdmin(userSub))
            QueryUtils.prepareQueryForEntitiesWithoutAuthentication(queryParams, offset, limit, contexts)
        else
            QueryUtils.prepareQueryForEntitiesWithAuthentication(queryParams, offset, limit, contexts)

        val result = neo4jClient.query(query).bind(USER_PREFIX + userSub).to("userId").fetch().all()
        return if (limit == 0)
            Pair(
                (result.firstOrNull()?.get("count") as Long?)?.toInt() ?: 0,
                emptyList()
            )
        else
            Pair(
                (result.firstOrNull()?.get("count") as Long?)?.toInt() ?: 0,
                result.map { (it["id"] as String).toUri() }
            )
    }
}
