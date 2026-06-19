package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.isWildcardTypeSelection
import com.egm.stellio.shared.queryparameter.QNode
import com.egm.stellio.shared.queryparameter.toSqlJsonPath

fun String.quote(): String =
    "\"".plus(this).plus("\"")

fun String.escapeSingleQuotes(): String =
    this.replace("'", "''")

fun Iterable<String>.toTypeSelection() = this.joinToString(",")

// Work for String and URI
// can't do two functions with the same name since Uri are String in the jvm
fun Iterable<Any>.toSqlList(): String = "(${this.joinToString(",") { "'$it'"} })"

fun Iterable<Any?>.toSqlArray(): String = "ARRAY[${this.joinToString(",") { "'$it'"} }]"
fun Sequence<String?>.toSqlArray(): String = this.toList().toSqlArray()

fun buildTypeQuery(rawQuery: String, columnName: String = "types", target: List<ExpandedTerm>? = null): String? =
    if (rawQuery.isBlank() || rawQuery.isWildcardTypeSelection()) null
    else rawQuery.replace(typeSelectionRegex) { matchResult ->
        """
        #{TARGET}# && ARRAY['${matchResult.value}']
        """.trimIndent()
    }
        .replace(";", " AND ")
        .replace("|", " OR ")
        .replace(",", " OR ")
        .let {
            if (target != null)
                it.replace("#{TARGET}#", target.toSqlArray())
            else
                it.replace("#{TARGET}#", columnName)
        }

/**
 * Transforms an NGSI-LD Query Language parameter as per clause 4.9 to a PostgreSQL SQL expression.
 * Uses a recursive descent parser to build an AST, then translates the AST to SQL jsonb_path_exists calls.
 */
fun buildQQuery(
    qNode: QNode,
    jsonKeys: Set<String> = emptySet(),
    expandValues: Set<String> = emptySet(),
    contexts: List<String>,
    target: ExpandedEntity? = null
): String =
    qNode.toSqlJsonPath(jsonKeys, expandValues, contexts, target)

fun buildScopeQQuery(scopeQQuery: String, target: ExpandedEntity? = null, columnName: String = "scopes"): String =
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
                it.replace("#{TARGET}#", columnName)
            else {
                val scopesArray = target.getScopes()?.toSqlArray()
                it.replace("#{TARGET}#", "$scopesArray")
            }
        }
