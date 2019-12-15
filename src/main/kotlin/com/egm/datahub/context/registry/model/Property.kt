package com.egm.datahub.context.registry.model

import com.fasterxml.jackson.annotation.JsonRawValue
import org.neo4j.ogm.annotation.Id
import org.neo4j.ogm.annotation.NodeEntity
import java.time.OffsetDateTime
import java.util.*

@NodeEntity
class Property(
    @Id
    var id: String = "urn:ngsi-ld:Property:${UUID.randomUUID()}",
    val name: String,
    var unitCode: String? = null,
    var observedAt: OffsetDateTime? = null,
    @JsonRawValue
    var value: Any
) : RelationshipTarget
