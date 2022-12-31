package com.egm.stellio.shared.util

import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

val formatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

fun ZonedDateTime.toNgsiLdFormat(): String =
    formatter.format(this)

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
