package com.egm.stellio.entity.util

import java.net.URLDecoder
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime

fun extractComparaisonParametersFromQuery(query: String): ArrayList<String> {
    val splitted = ArrayList<String>()
    if (query.contains("==")) {
        splitted.add(query.split("==")[0])
        splitted.add("=")
        splitted.add(query.split("==")[1])
    } else if (query.contains("!=")) {
        splitted.add(query.split("!=")[0])
        splitted.add("<>")
        splitted.add(query.split("!=")[1])
    } else if (query.contains(">=")) {
        splitted.add(query.split(">=")[0])
        splitted.add(">=")
        splitted.add(query.split(">=")[1])
    } else if (query.contains(">")) {
        splitted.add(query.split(">")[0])
        splitted.add(">")
        splitted.add(query.split(">")[1])
    } else if (query.contains("<=")) {
        splitted.add(query.split("<=")[0])
        splitted.add("<=")
        splitted.add(query.split("<=")[1])
    } else if (query.contains("<")) {
        splitted.add(query.split("<")[0])
        splitted.add("<")
        splitted.add(query.split("<")[1])
    }
    return splitted
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
