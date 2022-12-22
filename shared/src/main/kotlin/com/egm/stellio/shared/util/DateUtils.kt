package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.ExpandedTerm
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

val formatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

fun ZonedDateTime.toNgsiLdFormat(): String =
    formatter.format(this)

fun ZonedDateTime.toExpandedDateTime(attributeName: ExpandedTerm): Map<String, Map<String, String>> =
    mapOf(
        attributeName to mapOf(
            JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_DATE_TIME_TYPE,
            JsonLdUtils.JSONLD_VALUE_KW to this.toNgsiLdFormat()
        )
    )

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
