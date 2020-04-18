package com.egm.stellio.shared.model

import com.fasterxml.jackson.databind.JsonNode
import java.time.OffsetDateTime

data class Notification(
    val id: String,
    val type: String = "Notification",
    val notifiedAt: OffsetDateTime,
    val subscriptionId: String,
    val data: List<JsonNode>
)
