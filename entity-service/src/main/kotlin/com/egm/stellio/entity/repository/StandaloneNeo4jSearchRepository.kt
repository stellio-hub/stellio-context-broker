package com.egm.stellio.entity.repository

import com.egm.stellio.shared.util.toUri
import org.neo4j.ogm.session.Session
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI

@Component
@ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
class StandaloneNeo4jSearchRepository(
    private val session: Session
) : SearchRepository {

    override fun getEntities(
        params: Map<String, Any?>,
        userId: String,
        page: Int,
        limit: Int,
        contexts: List<String>
    ): Pair<Int, List<URI>> {
        val query = QueryUtils.prepareQueryForEntitiesWithoutAuthentication(params, page, limit, contexts)
        val result = session.query(query, emptyMap<String, Any>(), true)

        return Pair(
            (result.firstOrNull()?.get("count") as Long?)?.toInt() ?: 0,
            result.map { (it["id"] as String).toUri() }
        )
    }
}
