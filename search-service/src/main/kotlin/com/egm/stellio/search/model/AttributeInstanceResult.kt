package com.egm.stellio.search.model

import java.time.ZonedDateTime

sealed class AttributeInstanceResult

data class FullAttributeInstanceResult(val payload: String) : AttributeInstanceResult()

data class SimplifiedAttributeInstanceResult(
    val value: Any,
    val observedAt: ZonedDateTime
) : AttributeInstanceResult()
