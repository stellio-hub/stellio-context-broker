package com.egm.stellio.entity.repository

import com.egm.stellio.entity.util.*
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_CAN_WRITE
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
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
        val matchEntityClause = buildMatchEntityClause(expandedType, prefix = "")
        val idClause = buildIdClause(id)
        val idPatternClause = buildIdPatternClause(idPattern)

        val finalFilterClause = setOf(idClause, idPatternClause, qClause, attrsClause)
            .filter { it.isNotEmpty() }
            .joinToString(separator = " AND ", prefix = " AND ")
            .takeIf { it.trim() != "AND" } ?: ""

        val matchAuthorizedEntitiesClause =
            """
            CALL {
                MATCH (user)
                WHERE user.id IN ${'$'}userAndGroupIds
                WITH user
                MATCH (user)-[]->()-
                    [:$AUTH_TERM_CAN_READ|:$AUTH_TERM_CAN_WRITE|:$AUTH_TERM_CAN_ADMIN]->$matchEntityClause
                RETURN collect(id(entity)) as entitiesIds
                UNION
                MATCH $matchEntityClause-[:HAS_VALUE]->(prop:Property { name: '$AUTH_PROP_SAP' })
                WHERE prop.value IN [
                    '${AuthContextModel.SpecificAccessPolicy.AUTH_WRITE.name}',
                    '${AuthContextModel.SpecificAccessPolicy.AUTH_READ.name}'
                ]
                RETURN collect(id(entity)) as entitiesIds
            }
            WITH entitiesIds
            MATCH (entity)
            WHERE id(entity) IN entitiesIds
            $finalFilterClause
            """.trimIndent()

        val pagingClause = if (limit == 0)
            """
            RETURN count(entity.id) as count
            """.trimIndent()
        else
            """
                WITH collect(distinct(entity.id)) as entityIds, count(entity.id) as count
                UNWIND entityIds as id
                RETURN id, count
                ORDER BY id
                SKIP $offset LIMIT $limit
            """.trimIndent()

        return """
            $matchAuthorizedEntitiesClause
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
            WITH collect(entity.id) as entitiesIds, count(entity) as count
            UNWIND entitiesIds as entityId
            RETURN entityId as id, count
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
                           WHERE p.name = '${expandJsonLdTerm(comparablePropertyPath[0], contexts)!!}'
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
                        NOT isEmpty((entity)-[:HAS_VALUE]->(:Property { name: '$expandedAttr' })) OR 
                        NOT isEmpty((entity)-[:HAS_OBJECT]-(:Relationship:`$expandedAttr`))
                   ) 
               }
            """.trimIndent()
        }

    fun prepareQueryForAuthorizedEntitiesWithAuthentication(
        queryParams: QueryParams,
        offset: Int,
        limit: Int
    ): String {
        val (_, expandedType, _, q, _) = queryParams
        val matchEntityClause = buildMatchEntityClause(expandedType, prefix = "")
        val authTerm = buildAuthTerm(q)
        val matchAuthorizedEntitiesClause =
            """
            CALL {
                MATCH (user)
                WHERE user.id IN ${'$'}userAndGroupIds
                WITH user
                MATCH (user)-[]->()-[right:$authTerm]->$matchEntityClause
                OPTIONAL MATCH $matchEntityClause-[:HAS_VALUE]->(prop:Property { name: '$AUTH_PROP_SAP' })
                WHERE prop.value IN [
                    '${AuthContextModel.SpecificAccessPolicy.AUTH_READ.name}'
                ]
                RETURN {entity: entity, right: right, specificAccessPolicy: prop.value} as entities 
            }
            WITH collect(distinct(entities)) as entities, count(distinct(entities)) as count 
            """.trimIndent()

        val pagingClause = if (limit == 0)
            """
            RETURN count
            """.trimIndent()
        else
            """
            RETURN entities, count
            ORDER BY entities
            SKIP $offset LIMIT $limit
            """.trimIndent()

        return """
            $matchAuthorizedEntitiesClause
            $pagingClause
            """
    }

    fun prepareQueryForAuthorizedEntitiesWithoutAuthentication(
        queryParams: QueryParams,
        offset: Int,
        limit: Int
    ): String {
        val (_, expandedType, _, _, _) = queryParams
        val matchEntityClause = buildMatchEntityClause(expandedType)
        val pagingClause = if (limit == 0)
            """
            RETURN count(entity) as count
            """.trimIndent()
        else
            """
            WITH collect(entity) as entities, count(entity) as count
            RETURN {entity: entities, right: "rCanWrite"} as entities, count
            ORDER BY entities
            SKIP $offset LIMIT $limit
            """.trimIndent()

        return """
            $matchEntityClause
            $pagingClause
            """
    }

    fun buildAuthTerm(q: String?): String =
        if (q == null)
            "$AUTH_TERM_CAN_READ|$AUTH_TERM_CAN_WRITE|$AUTH_TERM_CAN_ADMIN"
        else q.replace(qPattern.toRegex()) {
            matchResult ->
            matchResult.value
        }
            .replace(";", "|")
}
