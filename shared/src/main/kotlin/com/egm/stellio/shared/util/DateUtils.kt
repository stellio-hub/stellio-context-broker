package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import java.time.LocalDate
import java.time.LocalTime
import java.time.Period
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

fun periodDurationToTimeBucket(periodDuration: String): String {
    try {
        val timeBucket = StringBuilder()
        val aggrPeriod =
            if (periodDuration.contains("T")) periodDuration.split("T")[0]
            else periodDuration

        val aggrDuration =
            if (periodDuration.contains("T")) periodDuration.split("T")[1]
            else null

        if (aggrPeriod != "P") {
            val period = Period.parse(aggrPeriod)
            val years = "${period.years} years"
            val months = "${period.months} months"
            val days = "${period.days} days"

            timeBucket.append("$years $months $days")
        }

        if (aggrDuration != null) {
            val duration = java.time.Duration.parse("PT$aggrDuration")
            val hours = "${duration.toHoursPart()} hours"
            val minutes = "${duration.toMinutesPart()} minutes"
            val seconds = "${duration.toSecondsPart()} seconds"

            timeBucket.append(" $hours $minutes $seconds")
        }

        return timeBucket.toString()
    } catch (e: DateTimeParseException) {
        throw BadRequestDataException("'aggrPeriodDuration' parameter is not a valid duration of the period")
    }
}
