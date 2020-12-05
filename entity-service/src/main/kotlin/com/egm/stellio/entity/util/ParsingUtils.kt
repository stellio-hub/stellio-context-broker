package com.egm.stellio.entity.util

import com.egm.stellio.shared.model.OperationNotSupportedException
import java.net.URLDecoder
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime

fun splitQueryTermOnOperator(queryTerm: String): List<String> =
    queryTerm.split("==", "!=", ">=", ">", "<=", "<")

fun extractComparisonParametersFromQuery(query: String): Triple<String, String, String> {
    return when {
        query.contains("==") -> {
            Triple(query.split("==")[0], "=", query.split("==")[1])
        }
        query.contains("!=") -> {
            Triple(query.split("!=")[0], "<>", query.split("!=")[1])
        }
        query.contains(">=") -> {
            Triple(query.split(">=")[0], ">=", query.split(">=")[1])
        }
        query.contains(">") -> {
            Triple(query.split(">")[0], ">", query.split(">")[1])
        }
        query.contains("<=") -> {
            Triple(query.split("<=")[0], "<=", query.split("<=")[1])
        }
        query.contains("<") -> {
            Triple(query.split("<")[0], "<", query.split("<")[1])
        }
        else -> throw OperationNotSupportedException("Unsupported query term : $query")
    }
}

fun String.decode(): String =
    URLDecoder.decode(this, "UTF-8")

fun List<String>.decode(): List<String> =
    this.map {
        it.decode()
    }

fun String.isFloat(): Boolean =
    this.matches("-?\\d+(\\.\\d+)?".toRegex())

fun String.isDateTime(): Boolean =
    try {
        ZonedDateTime.parse(this)
        true
    } catch (e: Exception) {
        false
    }

fun String.isDate(): Boolean =
    try {
        LocalDate.parse(this)
        true
    } catch (e: Exception) {
        false
    }

fun String.isTime(): Boolean =
    try {
        LocalTime.parse(this)
        true
    } catch (e: Exception) {
        false
    }
