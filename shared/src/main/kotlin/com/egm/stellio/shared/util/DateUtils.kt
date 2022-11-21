package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.ExpandedTerm
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

val formatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

const val TEMPLATE: String = "\"YYYY-MM-DD\\\"T\\\"HH24:MI:SS.US\\\"Z\\\"\""

fun ZonedDateTime.toNgsiLdFormat(): String =
    formatter.format(this)

fun ZonedDateTime.toExpandedDateTime(attributeName: ExpandedTerm): Map<String, Map<String, String>> =
    mapOf(
        attributeName to mapOf(
            JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_DATE_TIME_TYPE,
            JsonLdUtils.JSONLD_VALUE_KW to this.toNgsiLdFormat()
        )
    )
