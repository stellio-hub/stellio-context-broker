package com.egm.stellio.search.model

import com.egm.stellio.shared.util.toUri
import io.r2dbc.postgresql.codec.Json
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

data class AttributeInstance(
    val temporalEntityAttribute: UUID,
    val instanceId: URI = "urn:ngsi-ld:Instance:${UUID.randomUUID()}".toUri(),
    val observedAt: ZonedDateTime,
    val value: String? = null,
    val measuredValue: Double? = null,
    val payload: Json
) {
    companion object {
        fun generateRandomInstanceId() = "urn:ngsi-ld:Instance:${UUID.randomUUID()}".toUri()
    }
}
