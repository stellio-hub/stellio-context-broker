package com.egm.stellio.subscription.model

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import java.time.ZonedDateTime

data class NotificationParams(
    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    var attributes: List<String>?,
    val format: FormatType = FormatType.NORMALIZED,
    val endpoint: Endpoint,
    var status: StatusType?,
    @JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
    val timesSent: Int = 0,
    val lastNotification: ZonedDateTime?,
    val lastFailure: ZonedDateTime?,
    val lastSuccess: ZonedDateTime?
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
