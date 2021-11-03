package com.egm.stellio.entity.model

import java.net.URI

class AttributeTypeInfo(
    val id: URI,
    val type: String = "Attribute",
    val attributeName: String,
    val attributeTypes: List<String>,
    val typeNames: List<String>,
    val attributeCount: Int
)
