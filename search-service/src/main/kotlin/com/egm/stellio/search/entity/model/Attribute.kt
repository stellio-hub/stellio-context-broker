package com.egm.stellio.search.entity.model

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_LANGUAGEMAP_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_VALUES
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_JSONPROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_JSONPROPERTY_VALUES
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LANGUAGEPROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LANGUAGEPROPERTY_VALUES
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NONE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NULL
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUES
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_OBJECTS
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_VOCABPROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_VOCABPROPERTY_VALUES
import io.r2dbc.postgresql.codec.Json
import org.springframework.data.annotation.Id
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

data class Attribute(
    @Id
    val id: UUID = UUID.randomUUID(),
    val entityId: URI,
    val attributeName: ExpandedTerm,
    val attributeType: AttributeType = AttributeType.Property,
    val attributeValueType: AttributeValueType,
    val datasetId: URI? = null,
    val createdAt: ZonedDateTime,
    val modifiedAt: ZonedDateTime? = null,
    val deletedAt: ZonedDateTime? = null,
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
        URI,
        JSON
    }

    enum class AttributeType {
        Property,
        Relationship,
        GeoProperty,
        JsonProperty,
        LanguageProperty,
        VocabProperty;

        fun toExpandedName(): String =
            when (this) {
                Property -> NGSILD_PROPERTY_TYPE.uri
                Relationship -> NGSILD_RELATIONSHIP_TYPE.uri
                GeoProperty -> NGSILD_GEOPROPERTY_TYPE.uri
                JsonProperty -> NGSILD_JSONPROPERTY_TYPE.uri
                LanguageProperty -> NGSILD_LANGUAGEPROPERTY_TYPE.uri
                VocabProperty -> NGSILD_VOCABPROPERTY_TYPE.uri
            }

        /**
         * Returns the key of the member for the simplified representation of the attribute, as defined in 4.5.9
         */
        fun toSimplifiedRepresentationKey(): String =
            when (this) {
                Property -> NGSILD_PROPERTY_VALUES
                Relationship -> NGSILD_RELATIONSHIP_OBJECTS
                GeoProperty -> NGSILD_GEOPROPERTY_VALUES
                JsonProperty -> NGSILD_JSONPROPERTY_VALUES
                LanguageProperty -> NGSILD_LANGUAGEPROPERTY_VALUES
                VocabProperty -> NGSILD_VOCABPROPERTY_VALUES
            }

        fun toDeletedPayload(): Map<String, Any> {
            return when (this) {
                Property, GeoProperty, JsonProperty, VocabProperty ->
                    mapOf(
                        JSONLD_TYPE_TERM to this.name,
                        JSONLD_VALUE_TERM to NGSILD_NULL
                    )
                Relationship ->
                    mapOf(
                        JSONLD_TYPE_TERM to this.name,
                        JSONLD_OBJECT to NGSILD_NULL
                    )
                LanguageProperty ->
                    mapOf(
                        JSONLD_TYPE_TERM to this.name,
                        JSONLD_LANGUAGEMAP_TERM to mapOf(NGSILD_NONE_TERM to NGSILD_NULL)
                    )
            }
        }
    }
}
