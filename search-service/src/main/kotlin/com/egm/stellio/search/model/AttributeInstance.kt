package com.egm.stellio.search.model

import java.time.OffsetDateTime
import java.util.*

data class AttributeInstance(
    val temporalEntityAttribute: UUID,
    val instanceId: UUID = UUID.randomUUID(),
    val observedAt: OffsetDateTime,
    val value: String? = null,
    val measuredValue: Double? = null
)
