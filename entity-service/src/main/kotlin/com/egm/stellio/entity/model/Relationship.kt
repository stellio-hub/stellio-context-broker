package com.egm.stellio.entity.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.neo4j.ogm.annotation.Labels
import org.neo4j.ogm.annotation.NodeEntity
import java.time.ZonedDateTime

@NodeEntity
class Relationship(

    @Labels
    @JsonProperty("@type")
    val type: List<String>,

    observedAt: ZonedDateTime? = null
) : Attribute(attributeType = "Relationship", observedAt = observedAt)