package com.egm.stellio.search.model

import com.egm.stellio.shared.model.ExpandedTerm
import org.springframework.data.annotation.Id
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

data class TemporalEntityAttribute(
    @Id
    val id: UUID = UUID.randomUUID(),
    val entityId: URI,
    val attributeName: ExpandedTerm,
    val attributeType: AttributeType = AttributeType.Property,
    val attributeValueType: AttributeValueType,
    val datasetId: URI? = null,
    val createdAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),
    val modifiedAt: ZonedDateTime? = null,
    val payload: String
) {
    enum class AttributeValueType {
        MEASURE,
        ANY,
        GEOMETRY
    }

    enum class AttributeType {
        Property,
        Relationship,
        GeoProperty
    }
}
