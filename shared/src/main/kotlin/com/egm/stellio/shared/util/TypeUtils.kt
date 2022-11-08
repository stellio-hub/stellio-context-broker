package com.egm.stellio.shared.util

import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import java.time.format.DateTimeParseException

fun String.isDateTime(): Boolean =
    try {
        ZonedDateTime.parse(this)
        true
    } catch (e: DateTimeParseException) {
        false
    }

fun String.isDate(): Boolean =
    try {
        LocalDate.parse(this)
        true
    } catch (e: DateTimeParseException) {
        false
    }

fun String.isTime(): Boolean =
    try {
        LocalTime.parse(this)
        true
    } catch (e: DateTimeParseException) {
        false
    }

// FIXME kinda weak ... better options?
fun String.isRelationshipTarget(): Boolean =
    this.removePrefix("\"").startsWith("urn:")

fun String.isFloat(): Boolean =
    this.matches("-?\\d+(\\.\\d+)?".toRegex())
