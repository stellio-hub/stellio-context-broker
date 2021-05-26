package com.egm.stellio.entity.util

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import java.util.regex.Pattern

object QueryUtils {

    fun queryAuthorizedEntities(params: Map<String, Any?>, page: Int, limit: Int, contexts: List<String>): String {
        val ids = params["id"] as List<String>?
        val type = params["type"] as String
        val idPattern = params["idPattern"] as String?
        val rawQuery = params["q"] as String
        val formattedIds = ids?.map { "'$it'" }
        val pattern = Pattern.compile("([^();|]+)")
        val innerQuery = buildInnerQuery(rawQuery, pattern, contexts)
        val matchUserClause =
            """
            CALL {
                MATCH (userEntity:User) WHERE userEntity.id = ${'$'}userId RETURN userEntity
                UNION 
                MATCH (userEntity:Client) WHERE 
                    (userEntity)-[:HAS_VALUE]
                    ->(:Property { name: "${AuthorizationService.SERVICE_ACCOUNT_ID}", value: ${'$'}userId})
                RETURN userEntity
            }
            with userEntity
            """.trimIndent()

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

        val matchEntityClause =
            if (type.isEmpty())
                "(entity:Entity)"
            else
                "(entity:Entity:`$type`)"

        val matchEntitiesClause =
            """
            MATCH (userEntity)-[:HAS_OBJECT]->(right:Attribute:Relationship)-[]->$matchEntityClause
            WHERE any(r IN labels(right) WHERE r IN ${AuthorizationService.READ_RIGHT.map { "'$it'" }})
            $idClause
            $idPatternClause
            $innerQuery
            return entity.id as entityId
            """.trimIndent()

        val matchEntitiesFromGroupClause =
            """
            MATCH (userEntity)-[:HAS_OBJECT]->(:Attribute:Relationship)
            -[:isMemberOf]->(:Entity)-[:HAS_OBJECT]-(grpRight:Attribute:Relationship)-[]->$matchEntityClause
	        WHERE any(r IN labels(grpRight) WHERE r IN ${AuthorizationService.READ_RIGHT.map { "'$it'" }})
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

        return """
            CALL {
                $matchUserClause
                $matchEntitiesClause
                UNION
                $matchUserClause
                $matchEntitiesFromGroupClause
            }
            $pagingClause
            """
    }

    fun queryEntities(params: Map<String, Any?>, page: Int, limit: Int, contexts: List<String>): String {
        val ids = params["id"] as List<String>?
        val type = params["type"] as String
        val idPattern = params["idPattern"] as String?
        val rawQuery = params["q"] as String
        val formattedIds = ids?.map { "'$it'" }
        val pattern = Pattern.compile("([^();|]+)")
        val innerQuery = buildInnerQuery(rawQuery, pattern, contexts)

        val matchClause =
            if (type.isEmpty())
                "MATCH (n:Entity)"
            else
                "MATCH (n:`$type`)"

        val whereClause =
            if (innerQuery.isNotEmpty() || ids != null || idPattern != null) " WHERE "
            else ""

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

        val pagingClause =
            """
            WITH collect(n) as entities, count(n) as count
            UNWIND entities as n
            RETURN n.id as id, count
            ORDER BY id
            SKIP ${(page - 1) * limit} LIMIT $limit
            """.trimIndent()

        return """
            $matchClause
            $whereClause
                $idClause
                $idPatternClause
                $innerQuery
            $pagingClause
            """
    }

    private fun buildInnerQuery(rawQuery: String, pattern: Pattern, contexts: List<String>): String =

        rawQuery.replace(pattern.toRegex()) { matchResult ->
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
                val comparablePropertyPath = parsedQueryTerm.first.split(".")
                val comparablePropertyName = if (comparablePropertyPath.size > 1)
                    comparablePropertyPath[1]
                else
                    "value"
                """
                   EXISTS {
                       MATCH (n)-[:HAS_VALUE]->(p:Property)
                       WHERE p.name = '${expandJsonLdKey(comparablePropertyPath[0], contexts)!!}'
                       AND p.$comparablePropertyName ${parsedQueryTerm.second} $comparableValue
                   }
                """.trimIndent()
            }
        }
            .replace(";", " AND ")
            .replace("|", " OR ")
}
