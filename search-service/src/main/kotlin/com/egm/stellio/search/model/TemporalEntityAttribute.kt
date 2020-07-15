package com.egm.stellio.search.model

import java.net.URI
import java.util.UUID

data class TemporalEntityAttribute(
    val id: UUID = UUID.randomUUID(),
    val entityId: String,
    val type: String,
    val attributeName: String,
    val attributeValueType: AttributeValueType,
    val datasetId: URI? = null,
    // FIXME it should be not null, but we have existing data where the payload is not present
    val entityPayload: String? = null
) {
    enum class AttributeValueType {
        MEASURE,
        ANY
    }
}
