package com.egm.stellio.entity.model

import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_COORDINATES_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import org.neo4j.ogm.annotation.Id
import org.neo4j.ogm.annotation.Labels
import org.neo4j.ogm.annotation.NodeEntity
import org.neo4j.ogm.annotation.Relationship
import org.neo4j.ogm.annotation.typeconversion.Convert
import org.neo4j.ogm.types.spatial.GeographicPoint2d
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime

@NodeEntity
@JsonIgnoreProperties(ignoreUnknown = true)
class Entity(
    @Id
    @JsonProperty("@id")
    @Convert(UriConverter::class)
    val id: URI,

    @Labels
    @JsonProperty("@type")
    val type: List<String>,

    @JsonIgnore
    val createdAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),

    @JsonIgnore
    var modifiedAt: ZonedDateTime? = null,

    @JsonIgnore
    var location: GeographicPoint2d? = null,

    @Relationship(type = "HAS_VALUE")
    val properties: MutableList<Property> = mutableListOf(),

    @Relationship(type = "HAS_OBJECT")
    val relationships: MutableList<com.egm.stellio.entity.model.Relationship> = mutableListOf(),

    var contexts: List<String> = mutableListOf()

) {

    fun serializeCoreProperties(includeSysAttrs: Boolean): MutableMap<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        resultEntity[JSONLD_ID] = id
        resultEntity[JSONLD_TYPE] = type

        if (includeSysAttrs) {
            resultEntity[NGSILD_CREATED_AT_PROPERTY] = mapOf(
                JSONLD_TYPE to NGSILD_DATE_TIME_TYPE,
                JSONLD_VALUE_KW to createdAt
            )

            modifiedAt?.run {
                resultEntity[NGSILD_MODIFIED_AT_PROPERTY] = mapOf(
                    JSONLD_TYPE to NGSILD_DATE_TIME_TYPE,
                    JSONLD_VALUE_KW to this
                )
            }
        }
        location?.run {
            resultEntity[NGSILD_LOCATION_PROPERTY] = mapOf(
                JSONLD_TYPE to "GeoProperty",
                NGSILD_GEOPROPERTY_VALUE to mapOf(
                    JSONLD_TYPE to "Point",
                    NGSILD_COORDINATES_PROPERTY to listOf(this.longitude, this.latitude)
                )
            )
        }

        return resultEntity
    }
}
