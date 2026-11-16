package com.egm.stellio.search.discovery.model

import java.net.URI

data class AttributeDetails(
    val id: URI,
    val type: String = "Attribute",
    val attributeName: String,
    val typeNames: Set<String>
)

fun List<List<AttributeDetails>>.merge(): List<AttributeDetails> =
    singleOrNull() ?: flatten().groupBy { it.id }.map { (_, attributes) ->
        attributes.first().copy(typeNames = attributes.flatMap { it.typeNames }.sorted().toSet())
    }.sortedBy { it.id }
