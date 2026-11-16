package com.egm.stellio.search.discovery.model

import java.net.URI

data class EntityType(
    val id: URI,
    val type: String = "EntityType",
    val typeName: String,
    val attributeNames: List<String>
)

fun List<List<EntityType>>.merge(): List<EntityType> =
    singleOrNull() ?: flatten().groupBy { it.id }.map { (_, entityTypes) ->
        entityTypes.first().copy(attributeNames = entityTypes.flatMap { it.attributeNames }.distinct().sorted())
    }.sortedBy { it.id }
