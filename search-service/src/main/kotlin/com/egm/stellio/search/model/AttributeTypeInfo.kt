package com.egm.stellio.search.model

import java.net.URI

data class AttributeTypeInfo(
    val id: URI,
    val type: String = "Attribute",
    val attributeName: String,
    val attributeTypes: Set<AttributeType>,
    val typeNames: Set<String>,
    val attributeCount: Int
)
