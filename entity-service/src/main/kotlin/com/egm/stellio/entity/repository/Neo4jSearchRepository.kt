package com.egm.stellio.entity.repository

import arrow.core.Option
import com.egm.stellio.entity.authorization.Neo4jAuthorizationService
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.Sub
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
        val query = if (neo4jAuthorizationService.userIsAdmin(sub))
            QueryUtils.prepareQueryForEntitiesWithoutAuthentication(queryParams, contexts)
        else
            QueryUtils.prepareQueryForEntitiesWithAuthentication(queryParams, contexts)

        val userAndGroupIds = neo4jAuthorizationService.getSubjectGroups(sub)
            .plus(neo4jAuthorizationService.getSubjectUri(sub))
            .map { it.toString() }

        val result = neo4jClient
            .query(query)
            .bind(userAndGroupIds).to("userAndGroupIds")
            .fetch()
            .all()
        return prepareResults(queryParams.limit, result)
    }
}
