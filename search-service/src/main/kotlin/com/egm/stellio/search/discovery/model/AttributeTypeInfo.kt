package com.egm.stellio.search.discovery.model

import java.net.URI

data class AttributeTypeInfo(
    val id: URI,
    val type: String = "Attribute",
    val attributeName: String,
    val attributeTypes: Set<AttributeType>,
    val typeNames: Set<String>,
    val attributeCount: Int
)

// counts are summed even if some instances are known by several sources (estimates are allowed, TS 104 175 §10.6.2)
fun List<AttributeTypeInfo>.merge(): AttributeTypeInfo? =
    if (size <= 1) firstOrNull()
    else first().copy(
        attributeTypes = flatMap { it.attributeTypes }.sorted().toSet(),
        typeNames = flatMap { it.typeNames }.sorted().toSet(),
        attributeCount = sumOf { it.attributeCount }
    )
