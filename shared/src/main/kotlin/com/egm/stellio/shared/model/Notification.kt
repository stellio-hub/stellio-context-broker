package com.egm.stellio.shared.model

import java.net.URI
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.*

data class Notification(
    val id: URI = URI.create("urn:ngsi-ld:Notification:${UUID.randomUUID()}"),
    val type: String = "Notification",
    val subscriptionId: String,
    val notifiedAt: ZonedDateTime = ZonedDateTime.now(ZoneOffset.of("+02:00")),
    val data: List<Map<String, Any>>
)
