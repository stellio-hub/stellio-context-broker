package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.ZONE_OFFSET
import java.net.URI
import java.time.ZonedDateTime
import java.util.*

data class Notification(
    val id: URI = URI.create("urn:ngsi-ld:Notification:${UUID.randomUUID()}"),
    val type: String = "Notification",
    val subscriptionId: String,
    val notifiedAt: ZonedDateTime = ZonedDateTime.now(ZONE_OFFSET),
    val data: List<Map<String, Any>>
)
