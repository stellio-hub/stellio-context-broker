package com.egm.stellio.entity.model

import com.egm.stellio.entity.config.Neo4jUriPropertyConverter
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVATION_SPACE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OPERATION_SPACE_PROPERTY
import com.egm.stellio.shared.util.toNgsiLdFormat
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import org.springframework.data.annotation.LastModifiedDate
import org.springframework.data.neo4j.core.convert.ConvertWith
import org.springframework.data.neo4j.core.schema.DynamicLabels
import org.springframework.data.neo4j.core.schema.Id
import org.springframework.data.neo4j.core.schema.Node
import org.springframework.data.neo4j.core.schema.Relationship
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime

@Node
@JsonIgnoreProperties(ignoreUnknown = true)
data class Entity(
    @Id
    @JsonProperty("@id")
    @ConvertWith(converter = Neo4jUriPropertyConverter::class)
    val id: URI,

    @DynamicLabels
    @JsonProperty("@type")
    val types: List<String>,

    @JsonIgnore
    val createdAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),

    @JsonIgnore
    @LastModifiedDate
    var modifiedAt: ZonedDateTime? = null,

    @JsonIgnore
    var location: String? = null,

    @JsonIgnore
    var observationSpace: String? = null,

    @JsonIgnore
    var operationSpace: String? = null,

    @Relationship(type = "HAS_VALUE")
    val properties: MutableList<Property> = mutableListOf(),

    @Relationship(type = "HAS_OBJECT")
    val relationships: MutableList<com.egm.stellio.entity.model.Relationship> = mutableListOf(),

    var contexts: List<String> = mutableListOf()

) {

    fun serializeCoreProperties(includeSysAttrs: Boolean): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        resultEntity[JSONLD_ID] = id.toString()
        resultEntity[JSONLD_TYPE] = types

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
            // leave the WKT encoded value at this step,
            // it will be transformed in GeoJSON after JSON-LD compaction to not break the structure of the coordinates
            resultEntity[NGSILD_LOCATION_PROPERTY] = mapOf(
                JSONLD_TYPE to "GeoProperty",
                NGSILD_GEOPROPERTY_VALUE to this
            )
        }

        observationSpace?.run {
            // leave the WKT encoded value at this step,
            // it will be transformed in GeoJSON after JSON-LD compaction to not break the structure of the coordinates
            resultEntity[NGSILD_OBSERVATION_SPACE_PROPERTY] = mapOf(
                JSONLD_TYPE to "GeoProperty",
                NGSILD_GEOPROPERTY_VALUE to this
            )
        }

        operationSpace?.run {
            // leave the WKT encoded value at this step,
            // it will be transformed in GeoJSON after JSON-LD compaction to not break the structure of the coordinates
            resultEntity[NGSILD_OPERATION_SPACE_PROPERTY] = mapOf(
                JSONLD_TYPE to "GeoProperty",
                NGSILD_GEOPROPERTY_VALUE to this
            )
        }
        return resultEntity
    }
}
