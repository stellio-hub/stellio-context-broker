package com.egm.stellio.search.discovery.model

import java.net.URI

data class EntityTypeInfo(
    val id: URI,
    val type: String = "EntityTypeInfo",
    val typeName: String,
    val entityCount: Int,
    val attributeDetails: List<AttributeInfo>
)

// Called AttributeInfo (not Attribute as in 5.2.28) since there is already an Attribute class declared
data class AttributeInfo(
    val id: URI,
    val type: String = "Attribute",
    val attributeName: String,
    val attributeTypes: List<AttributeType>
)

enum class AttributeType(val key: String) {
    Property("Property"),
    Relationship("Relationship"),
    GeoProperty("GeoProperty"),
    JsonProperty("JsonProperty"),
    LanguageProperty("LanguageProperty"),
    VocabProperty("VocabProperty");

    companion object {
        fun forKey(key: String): AttributeType =
            entries.find { it.key == key }!!
    }
}
