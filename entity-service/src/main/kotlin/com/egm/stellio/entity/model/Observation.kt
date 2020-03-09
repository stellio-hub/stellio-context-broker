package com.egm.stellio.entity.model

import java.time.OffsetDateTime

data class Observation(
    val attributeName: String,
    val observedBy: String,
    val observedAt: OffsetDateTime,
    val value: Double,
    val unitCode: String,
    val latitude: Double?,
    val longitude: Double?
)