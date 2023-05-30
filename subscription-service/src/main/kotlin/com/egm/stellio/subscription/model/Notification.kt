package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NOTIFICATION_TERM
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

data class Notification(
    val id: URI = "urn:ngsi-ld:Notification:${UUID.randomUUID()}".toUri(),
    val type: String = NGSILD_NOTIFICATION_TERM,
    val subscriptionId: URI,
    val notifiedAt: ZonedDateTime = ngsiLdDateTime(),
    val data: List<Map<String, Any>>
)
