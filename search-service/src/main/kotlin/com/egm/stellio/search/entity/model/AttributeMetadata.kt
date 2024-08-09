package com.egm.stellio.search.entity.model

import com.egm.stellio.shared.model.WKTCoordinates
import java.net.URI
import java.time.ZonedDateTime

data class AttributeMetadata(
    val measuredValue: Double?,
    val value: String?,
    val geoValue: WKTCoordinates?,
    val valueType: Attribute.AttributeValueType,
    val datasetId: URI?,
    val type: Attribute.AttributeType,
    val observedAt: ZonedDateTime?
)
