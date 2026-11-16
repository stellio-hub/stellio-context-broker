package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.isWildcardTypeSelection
import com.egm.stellio.shared.queryparameter.QNode
import com.egm.stellio.shared.queryparameter.toSqlJsonPath
import com.egm.stellio.shared.util.ErrorMessages.QueryParameter.invalidScopeQQueryMessage

fun String.quote(): String =
    "\"".plus(this).plus("\"")

fun String.escapeSingleQuotes(): String =
    this.replace("'", "''")

fun Iterable<String>.toTypeSelection() = this.joinToString(",")

// Work for String and URI
// can't do two functions with the same name since Uri are String in the jvm
fun Iterable<Any>.toSqlList(): String =
    "(${this.joinToString(",") { "'${it.toString().escapeSingleQuotes()}'" }})"

fun Iterable<Any?>.toSqlArray(): String =
    "ARRAY[${this.joinToString(",") { "'${it.toString().escapeSingleQuotes()}'" }}]"
fun Sequence<String?>.toSqlArray(): String = this.toList().toSqlArray()

fun buildTypeQuery(rawQuery: String?, columnName: String = "types", target: List<ExpandedTerm>? = null): String? =
    if (rawQuery.isNullOrBlank() || rawQuery.isWildcardTypeSelection()) null
    else rawQuery.replace(typeSelectionRegex) { matchResult ->
        """
        #{TARGET}# && ARRAY['${matchResult.value.escapeSingleQuotes()}']
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
        }.let { "($it)" }

/**
 * Transforms an NGSI-LD Query Language parameter as per TS 104 175 §7.2.3 to a PostgreSQL SQL expression.
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

// ScopesQ grammar of TS 104 175 §7.2.5, except the leading "/" is optional as it is in the Scope grammar (§5.2.7),
// otherwise a scope like "a/b" could not be queried
// (see https://forge.etsi.org/rep/data/specifications/ts-104-175/-/work_items/66)
private const val SCOPEQ_LEVEL = """(?:\p{L}[\p{L}\p{N}_]*|\+)"""
private const val SCOPEQ = """/?$SCOPEQ_LEVEL(?:/$SCOPEQ_LEVEL)*"""
private const val OR_SCOPEQ = """(?:\($SCOPEQ(?:;$SCOPEQ)*\)|$SCOPEQ(?:/#)?)"""
private val scopeQRegex = """/#|$OR_SCOPEQ(?:[|,]$OR_SCOPEQ)*""".toRegex()

fun validateScopeQ(scopeQ: String?): Either<APIException, String?> =
    if (scopeQ == null || scopeQ.matches(scopeQRegex)) scopeQ.right()
    else BadRequestDataException(invalidScopeQQueryMessage(scopeQ)).left()

fun buildScopeQQuery(scopeQQuery: String, target: ExpandedEntity? = null, columnName: String = "scopes"): String =
    scopeQQuery.replace(scopeSelectionRegex) { matchResult ->
        if (matchResult.value.endsWith('#') || matchResult.value.contains('+'))
            """
            exists (select * from unnest(#{TARGET}#) as scope
            where scope similar to '${matchResult.value.toSimilarToPattern().escapeSingleQuotes()}')
            """.trimIndent()
        else
            """
            exists (select * from unnest(#{TARGET}#) as scope
            where scope = '${matchResult.value.escapeSingleQuotes()}')
            """.trimIndent()
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

// "_" is a SIMILAR TO wildcard but a valid scope character, and "/a/#" also matches "/a" itself
private fun String.toSimilarToPattern(): String =
    replace("_", "\\_")
        .replace("+", "[^/]+")
        .let { if (it.endsWith("/#")) it.removeSuffix("/#") + "(/%)?" else it }
