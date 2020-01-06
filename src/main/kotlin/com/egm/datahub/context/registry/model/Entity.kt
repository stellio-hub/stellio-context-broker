package com.egm.datahub.context.registry.model

import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.JSONLD_VALUE_KW
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_COORDINATES_PROPERTY
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_DATE_TIME_TYPE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_ENTITY_ID
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_ENTITY_TYPE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_GEOPROPERTY_VALUE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_LOCATION_PROPERTY
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.fasterxml.jackson.annotation.*
import org.neo4j.ogm.annotation.Id
import org.neo4j.ogm.annotation.Labels
import org.neo4j.ogm.annotation.NodeEntity
import org.neo4j.ogm.annotation.Relationship
import org.neo4j.ogm.types.spatial.GeographicPoint2d
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

    @JsonIgnore
    var location: GeographicPoint2d? = null,

    @Relationship(type = "HAS_VALUE")
    val properties: MutableList<Property> = mutableListOf(),

    @Relationship(type = "HAS_OBJECT")
    val relationships: MutableList<com.egm.datahub.context.registry.model.Relationship> = mutableListOf(),

    var contexts: List<String> = mutableListOf()

) {

    fun serializeCoreProperties(): MutableMap<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        resultEntity[NGSILD_ENTITY_ID] = id
        resultEntity[NGSILD_ENTITY_TYPE] = type
        resultEntity[NGSILD_CREATED_AT_PROPERTY] = mapOf(
            NGSILD_ENTITY_TYPE to NGSILD_DATE_TIME_TYPE,
            JSONLD_VALUE_KW to createdAt
        )

        modifiedAt?.run {
            resultEntity[NGSILD_MODIFIED_AT_PROPERTY] = mapOf(
                NGSILD_ENTITY_TYPE to NGSILD_DATE_TIME_TYPE,
                JSONLD_VALUE_KW to this
            )
        }

        location?.run {
            resultEntity[NGSILD_LOCATION_PROPERTY] = mapOf(
                NGSILD_ENTITY_TYPE to NGSILD_GEOPROPERTY_TYPE,
                NGSILD_GEOPROPERTY_VALUE to mapOf(
                    NGSILD_ENTITY_TYPE to "Point",
                    NGSILD_COORDINATES_PROPERTY to listOf(this.longitude, this.latitude)
                )
            )
        }

        return resultEntity
    }
}
