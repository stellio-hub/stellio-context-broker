package com.egm.stellio.shared.util

import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit

object DateUtils {
    val formatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

    fun ZonedDateTime.toNgsiLdFormat(): String =
        formatter.format(this)

    fun ZonedDateTime.toHttpHeaderFormat(): String =
        formatter.format(this)

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
}
