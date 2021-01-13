package com.egm.stellio.entity.model

import java.net.URI

data class EntityTypeInfo(
    val id: URI,
    val type: String = "EntityTypeInformation",
    val typeName: String,
    val entityCount: Int,
    val attributeDetails: List<AttributeInfo>
)

data class AttributeInfo(
    val id: URI,
    val type: String = "Attribute",
    val attributeName: String,
    val attributeTypes: List<String>
)
