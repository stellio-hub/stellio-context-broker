package com.egm.stellio.entity.repository

import arrow.core.Option
import com.egm.stellio.entity.authorization.Neo4jAuthorizationService
import com.egm.stellio.entity.model.Entity
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
        offset: Int,
        limit: Int,
        contexts: List<String>
    ): Pair<Int, List<URI>> {
        val query = if (neo4jAuthorizationService.userIsAdmin(sub))
            QueryUtils.prepareQueryForEntitiesWithoutAuthentication(queryParams, offset, limit, contexts)
        else
            QueryUtils.prepareQueryForEntitiesWithAuthentication(queryParams, offset, limit, contexts)

        val userAndGroupIds = neo4jAuthorizationService.getSubjectGroups(sub)
            .plus(neo4jAuthorizationService.getSubjectUri(sub))
            .map { it.toString() }

        val result = neo4jClient
            .query(query)
            .bind(userAndGroupIds).to("userAndGroupIds")
            .fetch()
            .all()
        return prepareResults(limit, result)
    }

    override fun getAuthorizedEntities(
        queryParams: QueryParams,
        sub: Option<Sub>,
        offset: Int,
        limit: Int,
        contexts: List<String>
    ): Pair<Int, List<Entity>> {
        val query = if (neo4jAuthorizationService.userIsAdmin(sub))
            QueryUtils.prepareQueryForAuthorizedEntitiesWithoutAuthentication(queryParams, offset, limit)
        else
            QueryUtils.prepareQueryForAuthorizedEntitiesWithAuthentication(queryParams, offset, limit)

        val userAndGroupIds = neo4jAuthorizationService.getSubjectGroups(sub)
            .plus(neo4jAuthorizationService.getSubjectUri(sub))
            .map { it.toString() }

        val result = neo4jClient
            .query(query)
            .bind(userAndGroupIds).to("userAndGroupIds")
            .fetch()
            .all()
        return prepareResultsEntities(limit, result)
    }
}
