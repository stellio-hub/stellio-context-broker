package com.egm.datahub.context.search.model

import java.time.OffsetDateTime
import javax.validation.constraints.NotNull

data class Observation(
    @NotNull val attributeName: String,
    @NotNull val observedBy: String,
    @NotNull val observedAt: OffsetDateTime,
    @NotNull val value: Double,
    @NotNull val unitCode: String,
    val latitude: Double?,
    val longitude: Double?
)
