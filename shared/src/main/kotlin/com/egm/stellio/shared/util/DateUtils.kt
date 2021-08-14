package com.egm.stellio.shared.util

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

val formatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

fun ZonedDateTime.toNgsiLdFormat(): String =
    formatter.format(this)
