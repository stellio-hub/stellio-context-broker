package com.egm.stellio.search.model

import java.net.URI
import java.time.ZonedDateTime

data class AttributeMetadata(
    val measuredValue: Double?,
    val value: String?,
    val valueType: TemporalEntityAttribute.AttributeValueType,
    val datasetId: URI?,
    val type: TemporalEntityAttribute.AttributeType,
    val createdAt: ZonedDateTime,
    val modifiedAt: ZonedDateTime?,
    val observedAt: ZonedDateTime?
)
