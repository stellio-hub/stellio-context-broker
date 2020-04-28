package com.egm.stellio.shared.model

import java.time.ZonedDateTime

data class Observation(
    val attributeName: String,
    val observedBy: String,
    val observedAt: ZonedDateTime,
    val value: Double,
    val unitCode: String,
    val latitude: Double?,
    val longitude: Double?
)
