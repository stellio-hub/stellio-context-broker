package com.egm.datahub.context.registry.model

import com.egm.datahub.context.registry.util.NgsiLdParsingUtils
import com.fasterxml.jackson.annotation.*
import org.neo4j.ogm.annotation.Id
import org.neo4j.ogm.annotation.Labels
import org.neo4j.ogm.annotation.NodeEntity
import org.neo4j.ogm.annotation.Relationship
import java.time.OffsetDateTime

@NodeEntity
@JsonIgnoreProperties(ignoreUnknown = true)
class Entity(
    @Id
    @JsonProperty("@id")
    val id: String,

    @Labels
    @JsonProperty("@type")
    val type: List<String>,

    @JsonIgnore
    val createdAt: OffsetDateTime = OffsetDateTime.now(),

    @JsonIgnore
    var modifiedAt: OffsetDateTime? = null,

    @Relationship(type = "HAS_VALUE")
    val properties: MutableList<Property> = mutableListOf(),

    @Relationship(type = "HAS_OBJECT")
    val relationships: MutableList<RelationshipTarget> = mutableListOf(),

    var contexts: List<String> = mutableListOf()

) : RelationshipTarget {

    fun serializeCoreProperties(): MutableMap<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        // TODO add other core NGSI-LD properties
        resultEntity[NgsiLdParsingUtils.NGSILD_ENTITY_ID] = id
        resultEntity[NgsiLdParsingUtils.NGSILD_ENTITY_TYPE] = type
        resultEntity[NgsiLdParsingUtils.NGSILD_CREATED_AT_PROPERTY] = mutableMapOf(
            "@type" to "https://uri.etsi.org/ngsi-ld/DateTime",
            "@value" to createdAt
        )

        if (modifiedAt != null)
            resultEntity[NgsiLdParsingUtils.NGSILD_MODIFIED_AT_PROPERTY] = mutableMapOf(
                "@type" to "https://uri.etsi.org/ngsi-ld/DateTime",
                "@value" to modifiedAt!!
            )

        return resultEntity
    }
}
