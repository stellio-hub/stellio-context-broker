package com.egm.onem2m.recorder.model

import java.time.ZonedDateTime
import javax.validation.constraints.NotNull

data class Measure(
        @NotNull val time: ZonedDateTime,
        @NotNull val container: String,
        @NotNull val contentInfo: String,
        @NotNull val unit: String,
        val content: Float
)
