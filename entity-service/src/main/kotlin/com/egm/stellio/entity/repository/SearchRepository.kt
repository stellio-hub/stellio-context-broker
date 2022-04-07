package com.egm.stellio.entity.repository

import arrow.core.Option
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.toUri
import org.neo4j.driver.internal.value.StringValue
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Relationship
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
        offset: Int,
        limit: Int,
        contexts: List<String>
    ): Pair<Int, List<URI>>

    @Transactional(readOnly = true)
    fun getAuthorizedEntities(
        queryParams: QueryParams,
        sub: Option<Sub>,
        offset: Int,
        limit: Int,
        contexts: List<String>
    ): Pair<Int, List<Entity>>

    fun prepareResults(limit: Int, result: Collection<Map<String, Any>>): Pair<Int, List<URI>> =
        if (limit == 0)
            Pair(
                (result.firstOrNull()?.get("count") as Long?)?.toInt() ?: 0,
                emptyList()
            )
        else Pair(
            (result.firstOrNull()?.get("count") as Long?)?.toInt() ?: 0,
            result.map { (it["id"] as String).toUri() }
        )

    fun prepareResultsEntities(limit: Int, result: Collection<Map<String, Any>>): Pair<Int, List<Entity>> =
        if (limit == 0)
            Pair(
                (result.firstOrNull()?.get("count") as Long?)?.toInt() ?: 0,
                emptyList()
            )
        else Pair(
            (result.firstOrNull()?.get("count") as Long?)?.toInt() ?: 0,
            (result.firstOrNull()?.get("entities") as List<Map<String, Any>>).map {
                val entityNode = it["entity"] as Node
                val rightOnEntity = (it["right"] as Relationship).type()
                Entity(
                    id = (entityNode.get("id") as StringValue).asString().toUri(),
                    type = entityNode.labels() as List<String>
                )
            }
        )
}
