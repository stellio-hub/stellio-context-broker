package com.egm.stellio.shared.util

import java.time.*
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit
import java.util.*

val formatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT
val httpHeaderFormatter =
    DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH).withZone(ZoneId.of("GMT"))

fun ZonedDateTime.toNgsiLdFormat(): String =
    formatter.format(this)

fun ZonedDateTime.toHttpHeaderFormat(): String =
    httpHeaderFormatter.format(this)

fun ngsiLdDateTime(): ZonedDateTime =
    Instant.now().truncatedTo(ChronoUnit.MICROS).atZone(ZoneOffset.UTC)

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
