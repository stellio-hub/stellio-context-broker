package com.egm.stellio.entity.repository

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.util.extractComparisonParametersFromQuery
import com.egm.stellio.entity.util.isDate
import com.egm.stellio.entity.util.isDateTime
import com.egm.stellio.entity.util.isFloat
import com.egm.stellio.entity.util.isRelationshipTarget
import com.egm.stellio.entity.util.isTime
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.JsonLdUtils.EGM_SPECIFIC_ACCESS_POLICY
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import java.util.regex.Pattern

object QueryUtils {

    private val qPattern = Pattern.compile("([^();|]+)")

    fun prepareQueryForEntitiesWithAuthentication(
        queryParams: QueryParams,
        offset: Int,
        limit: Int,
        contexts: List<String>
    ): String {
        val (id, expandedType, idPattern, q, expandedAttrs) = queryParams
        val qClause = q?.let { buildInnerQuery(it, contexts) } ?: ""
        val attrsClause = buildInnerAttrsFilterQuery(expandedAttrs)

        val matchUserClause =
            """
            CALL {
                MATCH (userEntity:Entity:`${AuthorizationService.USER_LABEL}`)
                    WHERE userEntity.id = ${'$'}userId RETURN userEntity
                UNION 
                MATCH (userEntity:Entity:`${AuthorizationService.CLIENT_LABEL}`)
                    WHERE (userEntity)-[:HAS_VALUE]
                        ->(:Property { name: "${AuthorizationService.SERVICE_ACCOUNT_ID}", value: ${'$'}userId})
                RETURN userEntity
            }
            with userEntity
            """.trimIndent()

        val matchEntityClause = buildMatchEntityClause(expandedType, prefix = "")
        val idClause = buildIdClause(id)
        val idPatternClause = buildIdPatternClause(idPattern)

        val finalFilterClause = setOf(idClause, idPatternClause, qClause, attrsClause)
            .filter { it.isNotEmpty() }
            .joinToString(separator = " AND ", prefix = " AND ")
            .takeIf { it.trim() != "AND" } ?: ""

        val matchEntitiesClause =
            """
            MATCH (userEntity)-[:HAS_OBJECT]->(right:Attribute:Relationship)-[]->$matchEntityClause
            WHERE any(r IN labels(right) WHERE r IN ${AuthorizationService.READ_RIGHT.map { "'$it'" }})
            $finalFilterClause
            return entity.id as entityId
            """.trimIndent()

        val matchEntitiesFromGroupClause =
            """
            MATCH (userEntity)-[:HAS_OBJECT]->(:Attribute:Relationship)
            -[:isMemberOf]->(:Entity)-[:HAS_OBJECT]-(grpRight:Attribute:Relationship)-[]->$matchEntityClause
	        WHERE any(r IN labels(grpRight) WHERE r IN ${AuthorizationService.READ_RIGHT.map { "'$it'" }})
            $finalFilterClause
            return entity.id as entityId
        """.trimIndent()

        val matchAuthorizedPerAccessPolicyClause =
            """
            MATCH $matchEntityClause-[:HAS_VALUE]->(prop:Property { name: "$EGM_SPECIFIC_ACCESS_POLICY" })
            WHERE prop.value IN [
                '${AuthorizationService.SpecificAccessPolicy.AUTH_WRITE.name}',
                '${AuthorizationService.SpecificAccessPolicy.AUTH_READ.name}'
            ]
            $finalFilterClause
            return entity.id as entityId
            """.trimIndent()

        val pagingClause = if (limit == 0)
            """
            RETURN count(entityId) as count
            """.trimIndent()
        else
            """
                WITH collect(distinct entityId) as entityIds, count(entityId) as count
                UNWIND entityIds as id
                RETURN id, count
                ORDER BY id
                SKIP $offset LIMIT $limit
            """.trimIndent()

        return """
            CALL {
                $matchAuthorizedPerAccessPolicyClause
                UNION
                $matchUserClause
                $matchEntitiesClause
                UNION
                $matchUserClause
                $matchEntitiesFromGroupClause
            }
            $pagingClause
            """
    }

    fun prepareQueryForEntitiesWithoutAuthentication(
        queryParams: QueryParams,
        offset: Int,
        limit: Int,
        contexts: List<String>
    ): String {
        val (id, expandedType, idPattern, q, expandedAttrs) = queryParams
        val qClause = q?.let { buildInnerQuery(it, contexts) } ?: ""
        val attrsClause = buildInnerAttrsFilterQuery(expandedAttrs)

        val matchEntityClause = buildMatchEntityClause(expandedType)
        val idClause = buildIdClause(id)
        val idPatternClause = buildIdPatternClause(idPattern)

        val finalFilterClause = setOf(idClause, idPatternClause, qClause, attrsClause)
            .filter { it.isNotEmpty() }
            .joinToString(separator = " AND ", prefix = " WHERE ")
            .takeIf { it.trim() != "WHERE" } ?: ""

        val pagingClause = if (limit == 0)
            """
            RETURN count(entity) as count
            """.trimIndent()
        else
            """
            WITH collect(entity) as entities, count(entity) as count
            UNWIND entities as entity
            RETURN entity.id as id, count
            ORDER BY id
            SKIP $offset LIMIT $limit
            """.trimIndent()

        return """
            $matchEntityClause
            $finalFilterClause
            $pagingClause
            """
    }

    private fun buildMatchEntityClause(expandedType: String?, prefix: String = "MATCH"): String =
        if (expandedType == null)
            "$prefix (entity:Entity)"
        else
            "$prefix (entity:`$expandedType`)"

    private fun buildIdClause(id: List<String>?): String {
        return if (id != null) {
            val formattedIds = id.map { "'$it'" }
            " entity.id in $formattedIds "
        } else ""
    }

    private fun buildIdPatternClause(idPattern: String?): String =
        if (idPattern != null)
            " entity.id =~ '$idPattern' "
        else ""

    private fun buildInnerQuery(rawQuery: String, contexts: List<String>): String =

        rawQuery.replace(qPattern.toRegex()) { matchResult ->
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
                if (listOf("createdAt", "modifiedAt").contains(parsedQueryTerm.first))
                    """
                        entity.${parsedQueryTerm.first} ${parsedQueryTerm.second} $comparableValue
                    """.trimIndent()
                else
                    """
                       EXISTS {
                           MATCH (entity)-[:HAS_VALUE]->(p:Property)
                           WHERE p.name = '${expandJsonLdKey(comparablePropertyPath[0], contexts)!!}'
                           AND p.$comparablePropertyName ${parsedQueryTerm.second} $comparableValue
                       }
                    """.trimIndent()
            }
        }
            .replace(";", " AND ")
            .replace("|", " OR ")

    private fun buildInnerAttrsFilterQuery(expandedAttrs: Set<String>): String =
        expandedAttrs.joinToString(
            separator = " AND "
        ) { expandedAttr ->
            """
               EXISTS {
                   MATCH (entity)
                   WHERE (
                        (entity)-[:HAS_VALUE]->(:Property { name: '$expandedAttr' })
                        OR 
                        (entity)-[:HAS_OBJECT]-(:Relationship:`$expandedAttr`)
                   ) 
               }
            """.trimIndent()
        }
}
