package com.egm.stellio.shared.model

import java.net.URI
import java.time.OffsetDateTime
import java.util.*

data class Notification(
    val id: URI = URI.create("urn:ngsi-ld:Notification:${UUID.randomUUID()}"),
    val type: String = "Notification",
    val subscriptionId: String,
    val notifiedAt: OffsetDateTime = OffsetDateTime.now(),
    val data: List<Map<String, Any>>
)
