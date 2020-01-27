package com.egm.datahub.context.subscription.model

import java.time.OffsetDateTime

data class NotificationParams(
    val attributes: List<String>,
    val format: FormatType = FormatType.NORMALIZED,
    val endpoint: Endpoint,
    var status: StatusType?,
    val timesSent: Int = 0,
    val lastNotification: OffsetDateTime?,
    val lastFailure: OffsetDateTime?,
    val lastSuccess: OffsetDateTime?
) {
    enum class FormatType(val format: String) {
        KEY_VALUES("keyValues"),
        NORMALIZED("normalized")
    }

    enum class StatusType(val status: String) {
        OK("ok"),
        FAILED("failed")
    }
}