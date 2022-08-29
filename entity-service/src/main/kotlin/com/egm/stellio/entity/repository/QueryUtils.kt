package com.egm.stellio.entity.repository

import com.egm.stellio.entity.util.*
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.GeoQuery
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_CAN_WRITE
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.extractGeorelParams
import com.egm.stellio.shared.util.qPattern
import com.egm.stellio.shared.util.verifGeoQuery
import java.net.URI
import java.util.regex.Pattern

object QueryUtils {

    fun prepareQueryForEntitiesWithAuthentication(
        queryParams: QueryParams,
        contexts: List<String>
    ): String {
        val qClause = queryParams.q?.let { buildInnerQuery(it, contexts) } ?: ""
        val attrsClause = buildInnerAttrFilterQuery(queryParams.attrs)
        val matchEntityClause = buildMatchEntityClause(queryParams.types, "")
        val idClause = buildIdsClause(queryParams.ids)
        val idPatternClause = buildIdPatternClause(queryParams.idPattern)

        val finalFilterClause = setOf(idClause, idPatternClause, qClause, attrsClause)
            .filter { it.isNotEmpty() }
            .joinToString(separator = " AND ", prefix = " AND ")
            .takeIf { it.trim() != "AND" } ?: ""

        val finalFilter =
            if (verifGeoQuery(queryParams.geoQuery)) {
                """
                ${buildFitlerWithGeoQuery(queryParams.geoQuery)}
                $finalFilterClause 
                """.trimIndent()
            } else finalFilterClause

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
            $finalFilter
            """.trimIndent()

        val pagingClause = if (queryParams.limit == 0)
            """
            RETURN count(entity.id) as count
            """.trimIndent()
        else
            """
                WITH collect(distinct(entity.id)) as entityIds, count(entity.id) as count
                UNWIND entityIds as id
                RETURN id, count
                ORDER BY id
                SKIP ${queryParams.offset} LIMIT ${queryParams.limit}                
            """.trimIndent()

        return """
                $matchAuthorizedEntitiesClause
                $pagingClause
        """.trimIndent()
    }

    fun prepareQueryForEntitiesWithoutAuthentication(
        queryParams: QueryParams,
        contexts: List<String>
    ): String {

        val qClause = queryParams.q?.let { buildInnerQuery(it, contexts) } ?: ""
        val attrsClause = buildInnerAttrFilterQuery(queryParams.attrs)

        val matchEntityClause = buildMatchEntityClause(queryParams.types)
        val idClause = buildIdsClause(queryParams.ids)
        val idPatternClause = buildIdPatternClause(queryParams.idPattern)

        val finalFilterClause = setOf(idClause, idPatternClause, qClause, attrsClause)
            .filter { it.isNotEmpty() }
            .joinToString(separator = " AND ", prefix = " WHERE ")
            .takeIf { it.trim() != "WHERE" } ?: ""

        val finalFilter =
            if (verifGeoQuery(queryParams.geoQuery)) {
                buildFitlerWithGeoQuery(queryParams.geoQuery)
                finalFilterClause
            } else finalFilterClause

        val pagingClause = if (queryParams.limit == 0)
            """
            RETURN count(entity) as count
            """.trimIndent()
        else """
            WITH collect(entity.id) as entitiesIds, count(entity) as count
            UNWIND entitiesIds as entityId
            RETURN entityId as id, count
            ORDER BY id
            SKIP ${queryParams.offset} LIMIT ${queryParams.limit}
        """.trimIndent()

        return """
                $matchEntityClause
                $finalFilter
                $pagingClause
        """.trimIndent()
    }

    fun buildMatchEntityClause(types: Set<ExpandedTerm>, prefix: String = "MATCH"): String =
        if (types.isEmpty())
            "$prefix (entity:Entity)"
        else
            "$prefix (entity:`${types.first()}`)"

    private fun buildIdsClause(ids: Set<URI>): String {
        return if (ids.isNotEmpty()) {
            val formattedIds = ids.map { "'$it'" }
            " entity.id in $formattedIds "
        } else ""
    }

    private fun buildIdPatternClause(idPattern: String?): String =
        if (idPattern != null)
            " entity.id =~ '$idPattern' "
        else ""

    private val innerRegexPattern: Pattern = Pattern.compile(".*(=~\"\\(\\?i\\)).*")

    private fun buildInnerQuery(rawQuery: String, contexts: List<String>): String {
        // Quick hack to allow inline options for regex expressions
        // (see https://keith.github.io/xcode-man-pages/re_format.7.html for more details)
        // When matched, parenthesis are replaced by special characters that are later restored after the main
        // qPattern regex has been processed
        val rawQueryWithPatternEscaped =
            if (rawQuery.matches(innerRegexPattern.toRegex())) {
                rawQuery.replace(innerRegexPattern.toRegex()) { matchResult ->
                    matchResult.value
                        .replace("(", "##")
                        .replace(")", "//")
                }
            } else rawQuery

        return rawQueryWithPatternEscaped.replace(qPattern.toRegex()) { matchResult ->
            // restoring the eventual inline options for regex expressions (replaced above)
            val fixedValue = matchResult.value
                .replace("##", "(")
                .replace("//", ")")
            val parsedQueryTerm = extractComparisonParametersFromQuery(fixedValue)
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
                           WHERE p.name = '${expandJsonLdTerm(comparablePropertyPath[0], contexts)}'
                           AND p.$comparablePropertyName ${parsedQueryTerm.second} $comparableValue
                       }
                    """.trimIndent()
            }
        }
            .replace(";", " AND ")
            .replace("|", " OR ")
    }

    private fun buildInnerAttrFilterQuery(attrs: Set<ExpandedTerm>): String =
        attrs.joinToString(
            separator = " AND "
        ) { attr ->
            """
               EXISTS {
                   MATCH (entity)
                   WHERE (
                        NOT isEmpty((entity)-[:HAS_VALUE]->(:Property { name: '$attr' })) OR 
                        NOT isEmpty((entity)-[:HAS_OBJECT]-(:Relationship:`$attr`))
                   ) 
               }
            """.trimIndent()
        }

    private fun buildFitlerWithGeoQuery(geoQuery: GeoQuery): String {
        val coordinates = geoQuery.coordinates.toString().split("[\\[,\\]]".toRegex())
        val georelParams = extractGeorelParams(geoQuery.georel!!)
        return """
                AND entity.location CONTAINS '${geoQuery.geometry!!.uppercase()}'
                AND point.distance(
                     point({
                        latitude: toFloat(apoc.text.regexGroups(entity.location, '([\\d\\.]+) ([\\d\\.]+)')[0][1]), 
                        longitude: toFloat(apoc.text.regexGroups(entity.location, '([\\d\\.]+) ([\\d\\.]+)')[0][2]),
                        crs: 'wgs-84'
                     }),
                     point({
                        latitude: ${coordinates[1].toFloat()}, 
                        longitude: ${coordinates[2].toFloat()}, 
                        crs: 'wgs-84'
                     })
                ) ${georelParams.second} ${georelParams.third}
        """.trimIndent()
    }
}
