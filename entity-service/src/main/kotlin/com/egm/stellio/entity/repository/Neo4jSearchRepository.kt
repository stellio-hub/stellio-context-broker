package com.egm.stellio.entity.repository

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.READ_RIGHT
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.SERVICE_ACCOUNT_ID
import com.egm.stellio.entity.util.QueryUtils.buildCypherQueryToQueryEntities
import com.egm.stellio.shared.util.toUri
import org.neo4j.ogm.session.Session
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI
import java.util.regex.Pattern

@Component
@ConditionalOnProperty("application.authentication.enabled")
class Neo4jSearchRepository(
    private val session: Session
) : SearchRepository {

    override fun getEntities(params: Map<String, Any?>, userId: String, page: Int, limit: Int): Pair<Int, List<URI>> {
        val ids = params["id"] as List<String>?
        val type = params["type"] as String
        val idPattern = params["idPattern"] as String?
        val rawQuery = params["q"] as String
        val formattedIds = ids?.map { "'$it'" }
        val pattern = Pattern.compile("([^();|]+)")
        val innerQuery = buildCypherQueryToQueryEntities(rawQuery, pattern)
        val idClause =
            if (ids != null) "AND entity.id in $formattedIds"
            else ""

        val idPatternClause =
            when {
                idPattern != null -> """
                            AND entity.id =~ '$idPattern'
                            ${if (innerQuery.isNotEmpty()) " AND " else ""}
                        """
                innerQuery.isNotEmpty() -> " AND "
                else -> ""
            }

        val matchUserClause =
            """
            MATCH (userEntity:User) WHERE (
                userEntity.id = ${'$'}userId OR 
                (userEntity)-[:HAS_VALUE]->(:Property { name: "$SERVICE_ACCOUNT_ID", value: ${'$'}userId})
            )
            with userEntity
            """.trimIndent()

        val entityClause =
            if (type.isEmpty())
                "(entity:Entity)"
            else
                "(entity:Entity:`$type`)"

        val matchEntitiesClause =
            """
            MATCH (userEntity)-[:HAS_OBJECT]->(right:Attribute:Relationship)-[]->$entityClause
            WHERE any(r IN labels(right) WHERE r IN ${READ_RIGHT.map { "'$it'" }})
            $idClause
            $idPatternClause
            $innerQuery
            return entity.id as entityId
            """.trimIndent()

        val matchEntitiesFromGroupClause =
            """
            MATCH (userEntity)-[:HAS_OBJECT]->(:Attribute:Relationship)-[:isMemberOf]->(:Entity)-[:HAS_OBJECT]-(grpRight:Attribute:Relationship)-[]->$entityClause
	        WHERE any(r IN labels(grpRight) WHERE r IN ${READ_RIGHT.map { "'$it'" }})
            $idClause
            $idPatternClause
            $innerQuery
            return entity.id as entityId
        """.trimIndent()

        val pagingClause =
            """
            WITH collect(entityId) as entityIds, count(entityId) as count
            UNWIND entityIds as id
            RETURN id, count
            ORDER BY id
            SKIP ${(page - 1) * limit} LIMIT $limit
            """.trimIndent()

        val finalQuery =
            """
            CALL {
                $matchUserClause
                $matchEntitiesClause
                UNION
                $matchUserClause
                $matchEntitiesFromGroupClause
            }
            $pagingClause
            """

        val result = session.query(finalQuery, mapOf("userId" to userId), true)
        return Pair(
            (result.firstOrNull()?.get("count") as Long?)?.toInt() ?: 0,
            result.map { (it["id"] as String).toUri() }
        )
    }
}
