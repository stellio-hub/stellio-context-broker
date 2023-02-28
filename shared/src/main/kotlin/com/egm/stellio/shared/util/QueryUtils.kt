package com.egm.stellio.shared.util

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

fun String.replaceSimpleQuote() =
    replace("'", "\"")

fun String.quote(): String =
    "\"".plus(this).plus("\"")

fun String.isCompoundAttribute(): Boolean =
    this.contains("\\[.*?]".toRegex())

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
                .replace("(", "##")
                .replace(")", "//")
        }
    } else this

fun String.unescapeRegexPattern(): String =
    this.replace("##", "(")
        .replace("//", ")")
