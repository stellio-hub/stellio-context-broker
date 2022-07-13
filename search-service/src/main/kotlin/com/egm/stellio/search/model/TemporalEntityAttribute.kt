package com.egm.stellio.search.model

import com.egm.stellio.shared.model.ExpandedTerm
import org.springframework.data.annotation.Id
import java.net.URI
import java.util.UUID

data class TemporalEntityAttribute(
    @Id
    val id: UUID = UUID.randomUUID(),
    val entityId: URI,
    val types: List<ExpandedTerm>,
    val attributeName: ExpandedTerm,
    val attributeType: AttributeType = AttributeType.Property,
    val attributeValueType: AttributeValueType,
    val datasetId: URI? = null
) {
    enum class AttributeValueType {
        MEASURE,
        ANY
    }

    enum class AttributeType {
        Property,
        Relationship
    }
}
