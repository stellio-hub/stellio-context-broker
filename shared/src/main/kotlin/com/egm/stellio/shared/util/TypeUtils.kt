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
