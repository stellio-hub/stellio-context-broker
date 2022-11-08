package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.OperationNotSupportedException

/**
 * Parse a query term to return a triple consisting of (attribute, operator, comparable value)
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
        queryTerm.contains("=~") ->
            Triple(queryTerm.split("like_regex")[0], "=~", queryTerm.split("=~")[1])
        else -> throw OperationNotSupportedException("Unsupported query term : $queryTerm")
    }
}

fun String.quoteIfNeeded() =
    if (this.isDate() || this.isDateTime() || this.isTime())
        "\"".plus(this).plus("\"")
    else
        this

fun String.replaceSimpleQuote() =
    replace("'", "\"")
