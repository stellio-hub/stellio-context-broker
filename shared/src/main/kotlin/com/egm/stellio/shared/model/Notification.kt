package com.egm.stellio.shared.model

import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

data class Notification(
    val id: URI = URI.create("urn:ngsi-ld:Notification:${UUID.randomUUID()}"),
    val type: String = "Notification",
    val subscriptionId: String,
    val notifiedAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),
    val data: List<Map<String, Any>>
)
