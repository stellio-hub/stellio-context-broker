package com.egm.stellio.search.model

import java.time.ZonedDateTime
import java.util.UUID

sealed class AttributeInstanceResult(open val temporalEntityAttribute: UUID)

data class FullAttributeInstanceResult(
    override val temporalEntityAttribute: UUID,
    val payload: String,
    val time: ZonedDateTime,
    val timeproperty: String,
    val sub: String?
) : AttributeInstanceResult(temporalEntityAttribute)

data class SimplifiedAttributeInstanceResult(
    override val temporalEntityAttribute: UUID,
    val value: Any,
    val time: ZonedDateTime
) : AttributeInstanceResult(temporalEntityAttribute)
