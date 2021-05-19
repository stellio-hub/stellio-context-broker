package com.egm.stellio.entity.repository

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.READ_RIGHT
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.SERVICE_ACCOUNT_ID
import com.egm.stellio.entity.util.*
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
        val innerQuery = rawQuery.replace(
            pattern.toRegex()
        ) { matchResult ->
            val parsedQueryTerm = extractComparisonParametersFromQuery(matchResult.value)
            if (parsedQueryTerm.third.isRelationshipTarget()) {
                """
                    EXISTS {
                        MATCH (entity)-[:HAS_OBJECT]-()-[:${parsedQueryTerm.first}]->(e)
                        WHERE e.id ${parsedQueryTerm.second} ${parsedQueryTerm.third}
                    }
                """.trimIndent()
            } else {
                val comparableValue = when {
                    parsedQueryTerm.third.isFloat() -> "toFloat('${parsedQueryTerm.third}')"
                    parsedQueryTerm.third.isDateTime() -> "datetime('${parsedQueryTerm.third}')"
                    parsedQueryTerm.third.isDate() -> "date('${parsedQueryTerm.third}')"
                    parsedQueryTerm.third.isTime() -> "localtime('${parsedQueryTerm.third}')"
                    else -> parsedQueryTerm.third
                }
                """
                   EXISTS {
                       MATCH (entity)-[:HAS_VALUE]->(p:Property)
                       WHERE p.name = '${parsedQueryTerm.first}'
                       AND p.value ${parsedQueryTerm.second} $comparableValue
                   }
                """.trimIndent()
            }
        }
            .replace(";", " AND ")
            .replace("|", " OR ")

        val matchClause =
            if (type.isEmpty())
                "MATCH (entity:Entity), (userEntity:Entity)"
            else
                "MATCH (entity:`$type`), (userEntity:Entity)"

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

        val whereClause =
            """
            WHERE (userEntity.id = ${'$'}userId 
            OR (userEntity)-[:HAS_VALUE]->(:Property { name: "$SERVICE_ACCOUNT_ID", value: ${'$'}userId }))
            """.trimIndent()

        val filterPermissionsClause =
            """
            MATCH (userEntity)-[:HAS_OBJECT]->(right:Attribute:Relationship)-[]->(entity:Entity)
            WHERE any(r IN labels(right) WHERE r IN ${READ_RIGHT.map { "'$it'" }})
            return entity
            UNION
            MATCH (userEntity)-[:HAS_OBJECT]->(:Attribute:Relationship)-[:isMemberOf]
                ->(:Entity)-[:HAS_OBJECT]-(grpRight:Attribute:Relationship)-[]->(entity:Entity)
            WHERE any(r IN labels(grpRight) WHERE r IN ${READ_RIGHT.map { "'$it'" }})
            return entity
            """.trimIndent()

        val finalQuery =
            """
            CALL {
                $matchClause
                $whereClause
                    $idClause
                    $idPatternClause
                    $innerQuery
                $filterPermissionsClause
            }
            WITH collect(entity) as entities, count(entity) as count
            UNWIND entities as entity
            RETURN entity.id as id, count
            """

        val result = session.query(finalQuery, mapOf("userId" to userId), true)
        return Pair(
            (result.firstOrNull()?.get("count") as Long?)?.toInt() ?: 0,
            result.map { (it["id"] as String).toUri() }
        )
    }
}
