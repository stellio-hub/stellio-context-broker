package com.egm.stellio.shared.util

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

val formatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

fun ZonedDateTime.toNgsiLdFormat(): String {
    val formatted = formatter.format(this)
    return if (this.nano == 0)
        formatted.removeSuffix("Z").plus(".000Z")
    else
        formatted
}
