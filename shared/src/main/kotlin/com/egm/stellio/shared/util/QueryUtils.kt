package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.OperationNotSupportedException

const val DATETIME_TEMPLATE: String = "\"YYYY-MM-DD\\\"T\\\"HH24:MI:SS.US\\\"Z\\\"\""

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
            Triple(queryTerm.split("=~")[0], "like_regex", queryTerm.split("=~")[1])
        else -> throw OperationNotSupportedException("Unsupported query term : $queryTerm")
    }
}

fun String.prepareDateValue(regexPattern: String) =
    if (this.isDate() || this.isDateTime() || this.isTime())
        if (regexPattern != "like_regex")
            "\"".plus(this).plus("\"").plus(".datetime($DATETIME_TEMPLATE)")
        else "\"".plus(this).plus("\"")
    else
        this

fun String.replaceSimpleQuote() =
    replace("'", "\"")
