package com.egm.stellio.entity.model

import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import com.fasterxml.jackson.annotation.JsonProperty
import org.neo4j.ogm.annotation.Labels
import org.neo4j.ogm.annotation.NodeEntity
import java.net.URI
import java.time.ZonedDateTime

@NodeEntity
class Relationship(

    @Labels
    @JsonProperty("@type")
    val type: List<String>,

    observedAt: ZonedDateTime? = null,

    datasetId: URI? = null

) : Attribute(attributeType = "Relationship", observedAt = observedAt, datasetId = datasetId)

fun String.toRelationshipTypeName(): String =
    this.extractShortTypeFromExpanded()
        .map {
            if (it.isUpperCase()) "_$it"
            else "${it.toUpperCase()}"
        }.joinToString("")

fun String.toNgsiLdRelationshipKey(): String =
    this.mapIndexed { index, c ->
        if (index > 1 && this[index - 1] == '_') "$c"
        else if (c == '_') ""
        else "${c.toLowerCase()}"
    }.joinToString("")
