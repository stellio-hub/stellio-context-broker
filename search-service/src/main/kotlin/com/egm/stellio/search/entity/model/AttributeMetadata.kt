package com.egm.stellio.search.entity.model

import com.egm.stellio.shared.model.WKTCoordinates
import io.r2dbc.postgresql.codec.Json
import java.net.URI
import java.time.ZonedDateTime

data class AttributeMetadata(
    val measuredValue: Double?,
    val value: Json?,
    val geoValue: WKTCoordinates?,
    val valueType: Attribute.AttributeValueType,
    val datasetId: URI?,
    val type: Attribute.AttributeType,
    val observedAt: ZonedDateTime?
)
