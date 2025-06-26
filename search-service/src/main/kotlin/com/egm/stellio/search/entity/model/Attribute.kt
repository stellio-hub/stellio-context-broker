package com.egm.stellio.search.entity.model

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JSONLD_LANGUAGE_KW
import com.egm.stellio.shared.model.JSONLD_NONE_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_IRI
import com.egm.stellio.shared.model.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.model.NGSILD_GEOPROPERTY_VALUE
import com.egm.stellio.shared.model.NGSILD_GEOPROPERTY_VALUES
import com.egm.stellio.shared.model.NGSILD_JSONPROPERTY_JSON
import com.egm.stellio.shared.model.NGSILD_JSONPROPERTY_JSONS
import com.egm.stellio.shared.model.NGSILD_JSONPROPERTY_TYPE
import com.egm.stellio.shared.model.NGSILD_LANGUAGEMAP_TERM
import com.egm.stellio.shared.model.NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP
import com.egm.stellio.shared.model.NGSILD_LANGUAGEPROPERTY_LANGUAGEMAPS
import com.egm.stellio.shared.model.NGSILD_LANGUAGEPROPERTY_TYPE
import com.egm.stellio.shared.model.NGSILD_NULL
import com.egm.stellio.shared.model.NGSILD_OBJECT_TERM
import com.egm.stellio.shared.model.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUES
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_OBJECTS
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.model.NGSILD_VALUE_TERM
import com.egm.stellio.shared.model.NGSILD_VOCABPROPERTY_TYPE
import com.egm.stellio.shared.model.NGSILD_VOCABPROPERTY_VOCAB
import com.egm.stellio.shared.model.NGSILD_VOCABPROPERTY_VOCABS
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedPropertyValue
import com.egm.stellio.shared.util.JsonUtils.serializeObject
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
    val modifiedAt: ZonedDateTime = createdAt,
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
         * Returns the expanded name of the member who holds the value of the attribute.
         *
         * For instance, https://uri.etsi.org/ngsi-ld/hasJSON if it is a JsonProperty
         */
        fun toExpandedValueMember(): String =
            when (this) {
                Property -> NGSILD_PROPERTY_VALUE
                Relationship -> NGSILD_RELATIONSHIP_OBJECT
                GeoProperty -> NGSILD_GEOPROPERTY_VALUE
                JsonProperty -> NGSILD_JSONPROPERTY_JSON
                LanguageProperty -> NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP
                VocabProperty -> NGSILD_VOCABPROPERTY_VOCAB
            }

        /**
         * Returns the key of the member for the simplified representation of the attribute, as defined in 4.5.9
         */
        fun toSimplifiedRepresentationKey(): String =
            when (this) {
                Property -> NGSILD_PROPERTY_VALUES
                Relationship -> NGSILD_RELATIONSHIP_OBJECTS
                GeoProperty -> NGSILD_GEOPROPERTY_VALUES
                JsonProperty -> NGSILD_JSONPROPERTY_JSONS
                LanguageProperty -> NGSILD_LANGUAGEPROPERTY_LANGUAGEMAPS
                VocabProperty -> NGSILD_VOCABPROPERTY_VOCABS
            }

        fun toNullCompactedRepresentation(datasetId: URI? = null): Map<String, Any> =
            when (this) {
                Property, GeoProperty, JsonProperty, VocabProperty ->
                    mapOf(
                        NGSILD_TYPE_TERM to this.name,
                        NGSILD_VALUE_TERM to NGSILD_NULL
                    )
                Relationship ->
                    mapOf(
                        NGSILD_TYPE_TERM to this.name,
                        NGSILD_OBJECT_TERM to NGSILD_NULL
                    )
                LanguageProperty ->
                    mapOf(
                        NGSILD_TYPE_TERM to this.name,
                        NGSILD_LANGUAGEMAP_TERM to mapOf(JSONLD_NONE_KW to NGSILD_NULL)
                    )
            }.let { nullAttrRepresentation ->
                if (datasetId != null)
                    nullAttrRepresentation.plus(
                        NGSILD_DATASET_ID_IRI to buildNonReifiedPropertyValue(datasetId.toString())
                    )
                else nullAttrRepresentation
            }

        fun toNullValue(): String =
            when (this) {
                Property, GeoProperty, JsonProperty, VocabProperty, Relationship -> NGSILD_NULL
                LanguageProperty ->
                    serializeObject(listOf(mapOf(JSONLD_VALUE_KW to NGSILD_NULL, JSONLD_LANGUAGE_KW to JSONLD_NONE_KW)))
            }
    }
}
