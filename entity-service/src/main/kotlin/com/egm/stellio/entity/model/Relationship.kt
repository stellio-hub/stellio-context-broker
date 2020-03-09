package com.egm.stellio.entity.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.neo4j.ogm.annotation.Labels
import org.neo4j.ogm.annotation.NodeEntity
import java.time.OffsetDateTime

@NodeEntity
class Relationship(

    @Labels
    @JsonProperty("@type")
    val type: List<String>,

    observedAt: OffsetDateTime? = null
) : Attribute(attributeType = "Relationship", observedAt = observedAt)