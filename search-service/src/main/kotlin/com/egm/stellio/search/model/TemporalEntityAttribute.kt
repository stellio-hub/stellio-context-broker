package com.egm.stellio.search.model

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_TYPE
import io.r2dbc.postgresql.codec.Json
import org.springframework.data.annotation.Id
import java.net.URI
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
    val createdAt: ZonedDateTime,
    val modifiedAt: ZonedDateTime? = null,
    val payload: Json
) {
    enum class AttributeValueType {
        NUMBER,
        OBJECT,
        ARRAY,
        STRING,
        BOOLEAN,
        GEOMETRY,
        DATETIME,
        DATE,
        TIME,
        URI
    }

    enum class AttributeType {
        Property,
        Relationship,
        GeoProperty;

        fun toNgsiLdExpanded(): String =
            when (this) {
                Property -> NGSILD_PROPERTY_TYPE.uri
                Relationship -> NGSILD_RELATIONSHIP_TYPE.uri
                GeoProperty -> NGSILD_GEOPROPERTY_TYPE.uri
            }
    }
}
