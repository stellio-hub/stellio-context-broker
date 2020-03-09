package com.egm.stellio.subscription.model

import java.time.OffsetDateTime
import java.util.*

data class Notification(
    val id: String = "urn:ngsi-ld:Notification:${UUID.randomUUID()}",
    val type: String = "Notification",
    val subscriptionId: String,
    val notifiedAt: OffsetDateTime = OffsetDateTime.now(),
    val data: List<String>
)