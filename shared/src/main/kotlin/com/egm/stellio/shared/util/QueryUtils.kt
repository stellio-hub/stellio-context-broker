package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import java.util.regex.Pattern

/**
 * Parse a query term to return a triple consisting of (attribute path, operator, value)
 */
fun extractComparisonParametersFromQuery(queryTerm: String): Triple<String, String, String> {
    return when {
        queryTerm.contains("==") ->
            Triple(queryTerm.split("==")[0], "==", queryTerm.split("==")[1])
        queryTerm.contains("!=") ->
            Triple(queryTerm.split("!=")[0], "<>", queryTerm.split("!=")[1])
        queryTerm.contains(">=") ->
            Triple(queryTerm.split(">=")[0], ">=", queryTerm.split(">=")[1])
        queryTerm.contains(">") ->
            Triple(queryTerm.split(">")[0], ">", queryTerm.split(">")[1])
        queryTerm.contains("<=") ->
            Triple(queryTerm.split("<=")[0], "<=", queryTerm.split("<=")[1])
        queryTerm.contains("<") ->
            Triple(queryTerm.split("<")[0], "<", queryTerm.split("<")[1])
        queryTerm.contains("!~=") ->
            // there is no such operator in PG JSON functions
            // it will be later transformed in a NOT(attrName like_regex "...")
            Triple(queryTerm.split("!~=")[0], "not_like_regex", queryTerm.split("!~=")[1])
        queryTerm.contains("~=") ->
            Triple(queryTerm.split("~=")[0], "like_regex", queryTerm.split("~=")[1])
        else ->
            // no operator found, it is a check for the existence of an attribute
            Triple(queryTerm, "", "")
    }
}

fun String.prepareDateValue() =
    if (this.isDate() || this.isDateTime() || this.isTime())
        this.quote()
    else
        this

fun String.quote(): String =
    "\"".plus(this).plus("\"")

fun String.isRange(): Boolean =
    this.contains("..")

fun String.rangeInterval(): Pair<Any, Any> =
    Pair(this.split("..")[0], this.split("..")[1])

// a string value could contain a comma ... to be improved
fun String.isValueList(): Boolean =
    this.contains(",")

fun String.listOfValues(): Set<String> =
    this.split(",").toSet()

fun String.parseAttributePath(): Pair<List<String>, List<String>> {
    val trailingPaths =
        if (this.contains("["))
            this.substringAfter('[').substringBefore(']').split(".")
        else emptyList()

    return Pair(
        this.substringBefore("[").split("."),
        trailingPaths
    )
}

private val innerRegexPattern: Pattern = Pattern.compile(".*(~=\"\\(\\?i\\)).*")

// Quick hack to allow inline options for regex expressions
// (see https://keith.github.io/xcode-man-pages/re_format.7.html for more details)
// When matched, parenthesis are replaced by special characters that are later restored after the main
// qPattern regex has been processed
fun String.escapeRegexpPattern(): String =
    if (this.matches(innerRegexPattern.toRegex())) {
        this.replace(innerRegexPattern.toRegex()) { matchResult ->
            matchResult.value
                .replace("(?i)", "##?i//")
        }
    } else this

fun String.unescapeRegexPattern(): String =
    this.replace("##", "(")
        .replace("//", ")")

fun buildTypeQuery(rawQuery: String): String =
    rawQuery.replace(typeSelectionRegex) { matchResult ->
        """
        types && ARRAY['${matchResult.value}']
        """.trimIndent()
    }
        .replace(";", " AND ")
        .replace("|", " OR ")
        .replace(",", " OR ")

// Transforms an NGSI-LD Query Language parameter as per clause 4.9 to a query supported by JsonPath.
fun buildQQuery(rawQuery: String, contexts: List<String>, target: JsonLdEntity? = null): String {
    val rawQueryWithPatternEscaped = rawQuery.escapeRegexpPattern()

    return rawQueryWithPatternEscaped.replace(qPattern.toRegex()) { matchResult ->
        // restoring the eventual inline options for regex expressions (replaced above)
        val fixedValue = matchResult.value.unescapeRegexPattern()

        val query = extractComparisonParametersFromQuery(fixedValue)

        val (mainAttributePath, trailingAttributePath) = query.first.parseAttributePath()
            .let { (attrPath, trailingPath) ->
                Pair(
                    attrPath.map { JsonLdUtils.expandJsonLdTerm(it, contexts) },
                    trailingPath.map { JsonLdUtils.expandJsonLdTerm(it, contexts) }
                )
            }
        val operator = query.second
        val value = query.third.prepareDateValue()

        transformQQueryToSqlJsonPath(
            mainAttributePath,
            trailingAttributePath,
            operator,
            value
        )
    }
        .replace(";", " AND ")
        .replace("|", " OR ")
        .replace("JSONPATH_OR_FILTER", "||")
        .let {
            if (target == null)
                it.replace("#{TARGET}#", "entity_payload.payload")
            else
                it.replace("#{TARGET}#", "'" + serializeObject(target.members) + "'")
        }
}

private fun transformQQueryToSqlJsonPath(
    mainAttributePath: List<ExpandedTerm>,
    trailingAttributePath: List<ExpandedTerm>,
    operator: String,
    value: String
) = when {
    mainAttributePath.size > 1 && !value.isURI() -> {
        val jsonAttributePath = mainAttributePath.joinToString(".") { "\"$it\"" }
        """
        jsonb_path_exists(#{TARGET}#,
            '$.$jsonAttributePath.**{0 to 2}."$JSONLD_VALUE" ? (@ $operator ${'$'}value)',
            '{ "value": $value }')
        """.trimIndent()
    }
    mainAttributePath.size > 1 && value.isURI() -> {
        val jsonAttributePath = mainAttributePath.joinToString(".") { "\"$it\"" }
        """
        jsonb_path_exists(#{TARGET}#,
            '$.$jsonAttributePath.**{0 to 2}."$JSONLD_ID" ? (@ $operator ${'$'}value)',
            '{ "value": ${value.quote()} }')
        """.trimIndent()
    }
    operator.isEmpty() ->
        """
        jsonb_path_exists(#{TARGET}#, '$."${mainAttributePath[0]}"')
        """.trimIndent()
    trailingAttributePath.isNotEmpty() -> {
        val jsonTrailingPath = trailingAttributePath.joinToString(".") { "\"$it\"" }
        """
        jsonb_path_exists(#{TARGET}#,
            '$."${mainAttributePath[0]}"."$NGSILD_PROPERTY_VALUE".$jsonTrailingPath.**{0 to 1}."$JSONLD_VALUE" ? 
                (@ $operator ${'$'}value)',
            '{ "value": $value }')
        """.trimIndent()
    }
    operator == "like_regex" ->
        """
        jsonb_path_exists(#{TARGET}#,
            '$."${mainAttributePath[0]}"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE" ? (@ like_regex $value)')
        """.trimIndent()
    operator == "not_like_regex" ->
        """
        NOT (jsonb_path_exists(#{TARGET}#,
            '$."${mainAttributePath[0]}"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE" ? (@ like_regex $value)'))
        """.trimIndent()
    value.isURI() ->
        """
        jsonb_path_exists(#{TARGET}#,
            '$."${mainAttributePath[0]}"."$NGSILD_RELATIONSHIP_HAS_OBJECT"."$JSONLD_ID" ? (@ $operator ${'$'}value)',
            '{ "value": ${value.quote()} }')
        """.trimIndent()
    value.isRange() -> {
        val (min, max) = value.rangeInterval()
        """
        jsonb_path_exists(#{TARGET}#,
            '$."${mainAttributePath[0]}"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE" ? 
                (@ >= ${'$'}min && @ <= ${'$'}max)',
            '{ "min": $min, "max": $max }')
        """.trimIndent()
    }
    value.isValueList() -> {
        // can't use directly the || operator since it will be replaced below when composing the filters
        // so use an intermediate JSONPATH_OR_FILTER placeholder
        val valuesFilter = value.listOfValues()
            .joinToString(separator = " JSONPATH_OR_FILTER ") { "@ == $it" }
        """
        jsonb_path_exists(#{TARGET}#,
            '$."${mainAttributePath[0]}"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE" ? ($valuesFilter)')
        """.trimIndent()
    }
    else ->
        """
        jsonb_path_exists(#{TARGET}#,
            '$."${mainAttributePath[0]}"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE" ? (@ $operator ${'$'}value)',
            '{ "value": $value }')
        """.trimIndent()
}

fun buildScopeQQuery(scopeQQuery: String, target: JsonLdEntity? = null): String =
    scopeQQuery.replace(scopeSelectionRegex) { matchResult ->
        when {
            matchResult.value.endsWith('#') ->
                """
                exists (select * from unnest(#{TARGET}#) as scope 
                where scope similar to '${matchResult.value.replace('#', '%')}')
                """.trimIndent()
            matchResult.value.contains('+') ->
                """
                exists (select * from unnest(#{TARGET}#) as scope 
                where scope similar to '${matchResult.value.replace("+", "[\\w\\d]+")}')
                """.trimIndent()
            else ->
                """
                exists (select * from unnest(#{TARGET}#) as scope where scope = '${matchResult.value}')
                """.trimIndent()
        }
    }
        .replace(";", " AND ")
        .replace("|", " OR ")
        .replace(",", " OR ")
        .let {
            if (target == null)
                it.replace("#{TARGET}#", "scopes")
            else {
                val scopesArray = target.getScopes()?.let { scopes ->
                    """
                    ARRAY[${scopes.joinToString(",") { "'$it'" } }]
                    """.trimIndent()
                }
                it.replace("#{TARGET}#", "$scopesArray")
            }
        }
