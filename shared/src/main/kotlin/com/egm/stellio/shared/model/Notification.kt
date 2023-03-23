package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.DateTimeSerializer
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NOTIFICATION_TERM
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import java.net.URI
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

data class Notification(
    val id: URI = "urn:ngsi-ld:Notification:${UUID.randomUUID()}".toUri(),
    val type: String = NGSILD_NOTIFICATION_TERM,
    val subscriptionId: URI,
    @JsonSerialize(using = DateTimeSerializer::class)
    val notifiedAt: ZonedDateTime = ZonedDateTime.now(ZoneOffset.UTC),
    val data: List<Map<String, Any>>
)
