package com.egm.stellio.search.discovery.model

import java.net.URI

data class EntityTypeInfo(
    val id: URI,
    val type: String = "EntityTypeInfo",
    val typeName: String,
    val entityCount: Int,
    val attributeDetails: List<AttributeInfo>
)

// counts are summed even if some entities are known by several sources (estimates are allowed, TS 104 175 §10.6.2)
fun List<EntityTypeInfo>.merge(): EntityTypeInfo? =
    if (size <= 1) firstOrNull()
    else first().copy(
        entityCount = sumOf { it.entityCount },
        attributeDetails = flatMap { it.attributeDetails }.groupBy { it.id }.map { (_, attributes) ->
            attributes.first().copy(attributeTypes = attributes.flatMap { it.attributeTypes }.distinct().sorted())
        }.sortedBy { it.attributeName }
    )

// Called AttributeInfo (not Attribute as in TS 104 175 §5.2.6.10.1) since there is already an Attribute class declared
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
    VocabProperty("VocabProperty"),
    ListProperty("ListProperty"),
    ListRelationship("ListRelationship");

    companion object {
        fun forKey(key: String): AttributeType =
            entries.find { it.key == key }!!
    }
}
