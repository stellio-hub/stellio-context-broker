package com.egm.stellio.search.model

import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

data class AttributeInstance(
    val temporalEntityAttribute: UUID,
    val instanceId: URI = URI.create("urn:ngsi-ld:Instance:${UUID.randomUUID()}"),
    val observedAt: ZonedDateTime,
    val value: String? = null,
    val measuredValue: Double? = null
)
