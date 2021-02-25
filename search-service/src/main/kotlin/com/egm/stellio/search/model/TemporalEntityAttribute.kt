package com.egm.stellio.search.model

import java.net.URI
import java.util.UUID

data class TemporalEntityAttribute(
    val id: UUID = UUID.randomUUID(),
    val entityId: URI,
    val type: String,
    val attributeName: String,
    val attributeValueType: AttributeValueType,
    val datasetId: URI? = null,
    // FIXME it should be removed since it is no longer used
    val entityPayload: String? = null
) {
    enum class AttributeValueType {
        MEASURE,
        ANY
    }
}
