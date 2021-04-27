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
import com.egm.stellio.shared.util.toNgsiLdFormat
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import org.locationtech.jts.io.WKTReader
import org.neo4j.ogm.annotation.Id
import org.neo4j.ogm.annotation.Labels
import org.neo4j.ogm.annotation.NodeEntity
import org.neo4j.ogm.annotation.Relationship
import org.neo4j.ogm.annotation.typeconversion.Convert
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
    var location: String? = null,

    @Relationship(type = "HAS_VALUE")
    val properties: MutableList<Property> = mutableListOf(),

    @Relationship(type = "HAS_OBJECT")
    val relationships: MutableList<com.egm.stellio.entity.model.Relationship> = mutableListOf(),

    var contexts: List<String> = mutableListOf()

) {

    fun serializeCoreProperties(includeSysAttrs: Boolean): MutableMap<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        resultEntity[JSONLD_ID] = id.toString()
        resultEntity[JSONLD_TYPE] = type

        if (includeSysAttrs) {
            resultEntity[NGSILD_CREATED_AT_PROPERTY] = mapOf(
                JSONLD_TYPE to NGSILD_DATE_TIME_TYPE,
                JSONLD_VALUE_KW to createdAt.toNgsiLdFormat()
            )

            modifiedAt?.run {
                resultEntity[NGSILD_MODIFIED_AT_PROPERTY] = mapOf(
                    JSONLD_TYPE to NGSILD_DATE_TIME_TYPE,
                    JSONLD_VALUE_KW to this.toNgsiLdFormat()
                )
            }
        }
        location?.run {
            val geometry = WKTReader().read(this)
            resultEntity[NGSILD_LOCATION_PROPERTY] = mapOf(
                JSONLD_TYPE to "GeoProperty",
                NGSILD_GEOPROPERTY_VALUE to mapOf(
                    JSONLD_TYPE to geometry.geometryType,
                    NGSILD_COORDINATES_PROPERTY to geometry.coordinates.map { listOf(it.x, it.y) }
                )
            )
        }
        return resultEntity
    }
}
