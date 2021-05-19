package com.egm.stellio.entity.repository

import com.egm.stellio.entity.util.*
import com.egm.stellio.shared.util.toUri
import org.neo4j.ogm.session.Session
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.net.URI
import java.util.regex.Pattern

@Component
@ConditionalOnProperty("application.authentication.enabled", havingValue = "false")
class StandaloneNeo4jSearchRepository(
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
                        MATCH (n)-[:HAS_OBJECT]-()-[:${parsedQueryTerm.first}]->(e)
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
                       MATCH (n)-[:HAS_VALUE]->(p:Property)
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
                "MATCH (n:Entity)"
            else
                "MATCH (n:`$type`)"

        val idClause =
            if (ids != null)
                """
                    n.id in $formattedIds
                    ${if (idPattern != null || innerQuery.isNotEmpty()) " AND " else ""}
                """
            else ""

        val idPatternClause =
            if (idPattern != null)
                """
                    n.id =~ '$idPattern'
                    ${if (innerQuery.isNotEmpty()) " AND " else ""}
                """
            else ""

        val whereClause =
            if (innerQuery.isNotEmpty() || ids != null || idPattern != null) " WHERE "
            else ""

        val pagingClause =
            """
            WITH collect(n) as entities, count(n) as count
            UNWIND entities as n
            RETURN n.id as id, count
            ORDER BY id
            SKIP ${(page - 1) * limit} LIMIT $limit
            """.trimIndent()

        val finalQuery =
            """
            $matchClause
            $whereClause
                $idClause
                $idPatternClause
                $innerQuery
            $pagingClause
            """

        val result = session.query(finalQuery, emptyMap<String, Any>(), true)

        return Pair(
            (result.firstOrNull()?.get("count") as Long?)?.toInt() ?: 0,
            result.map { (it["id"] as String).toUri() }
        )
    }
}
