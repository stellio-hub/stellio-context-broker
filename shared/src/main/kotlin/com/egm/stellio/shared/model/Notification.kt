package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.toUri
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

data class Notification(
    val id: URI = "urn:ngsi-ld:Notification:${UUID.randomUUID()}".toUri(),
    val type: String = "Notification",
    val subscriptionId: URI,
    val notifiedAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),
    val data: List<Map<String, Any>>
)
