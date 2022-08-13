package com.egm.stellio.search.model

import com.egm.stellio.shared.model.WKTCoordinates
import java.net.URI
import java.time.ZonedDateTime

data class AttributeMetadata(
    val measuredValue: Double?,
    val value: String?,
    val geoValue: WKTCoordinates?,
    val valueType: TemporalEntityAttribute.AttributeValueType,
    val datasetId: URI?,
    val type: TemporalEntityAttribute.AttributeType,
    val observedAt: ZonedDateTime?
)
