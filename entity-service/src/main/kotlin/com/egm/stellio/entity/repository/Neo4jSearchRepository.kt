package com.egm.stellio.entity.repository

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.USER_PREFIX
import com.egm.stellio.entity.authorization.Neo4jAuthorizationService
import com.egm.stellio.shared.util.toUri
import org.neo4j.ogm.session.Session
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled")
class Neo4jSearchRepository(
    private val session: Session,
    private val neo4jAuthorizationService: Neo4jAuthorizationService
) : SearchRepository {

    override fun getEntities(
        params: Map<String, Any?>,
        userSub: String,
        page: Int,
        limit: Int,
        contexts: List<String>
    ): Pair<Int, List<URI>> {
        val query = if (neo4jAuthorizationService.userIsAdmin(userSub))
            QueryUtils.prepareQueryForEntitiesWithoutAuthentication(params, page, limit, contexts)
        else
            QueryUtils.prepareQueryForEntitiesWithAuthentication(params, page, limit, contexts)

        val result = session.query(query, mapOf("userId" to USER_PREFIX + userSub), true)
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
