package com.egm.stellio.entity.model

import java.net.URI

data class EntityType(
    val id: URI,
    val type: String = "EntityType",
    val typeName: String,
    val attributeNames: List<String>
)
