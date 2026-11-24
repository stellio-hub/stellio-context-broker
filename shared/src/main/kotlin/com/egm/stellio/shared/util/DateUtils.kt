package com.egm.stellio.shared.util

import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit

private const val UTC_WHOLE_SECONDS_LENGTH = 19
private const val UTC_FRACTION_START = UTC_WHOLE_SECONDS_LENGTH + 1

val formatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

fun ZonedDateTime.toNgsiLdFormat(): String =
    formatter.format(this)

fun ngsiLdDateTime(): ZonedDateTime =
    Instant.now().truncatedTo(ChronoUnit.MICROS).atZone(ZoneOffset.UTC)

// NGSI-LD timestamps use UTC, so lexicographic order works across different seconds.
fun compareUtcTimestamps(left: String, right: String): Int {
    if (!left.regionMatches(0, right, 0, UTC_WHOLE_SECONDS_LENGTH)) {
        return left.compareTo(right)
    }

    // Within the same second, missing fractional digits compare as zeros.
    for (index in UTC_FRACTION_START until maxOf(left.lastIndex, right.lastIndex)) {
        val leftDigit = if (index < left.lastIndex) left[index] else '0'
        val rightDigit = if (index < right.lastIndex) right[index] else '0'
        if (leftDigit != rightDigit) return leftDigit - rightDigit
    }
    return 0
}

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
