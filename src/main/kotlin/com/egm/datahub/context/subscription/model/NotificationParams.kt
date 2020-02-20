package com.egm.datahub.context.subscription.model

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.OffsetDateTime

data class NotificationParams(
    var attributes: List<String>,
    val format: FormatType = FormatType.NORMALIZED,
    val endpoint: Endpoint,
    var status: StatusType?,
    val timesSent: Int = 0,
    val lastNotification: OffsetDateTime?,
    val lastFailure: OffsetDateTime?,
    val lastSuccess: OffsetDateTime?
) {
    enum class FormatType(val format: String) {
        @JsonProperty("keyValues")
        KEY_VALUES("keyValues"),
        @JsonProperty("normalized")
        NORMALIZED("normalized")
    }

    enum class StatusType(val status: String) {
        @JsonProperty("ok")
        OK("ok"),
        @JsonProperty("failed")
        FAILED("failed")
    }
}