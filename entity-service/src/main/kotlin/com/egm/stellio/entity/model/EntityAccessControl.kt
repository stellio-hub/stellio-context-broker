package com.egm.stellio.entity.model

import com.egm.stellio.entity.config.Neo4jUriPropertyConverter
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.toNgsiLdFormat
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import org.springframework.data.annotation.LastModifiedDate
import org.springframework.data.neo4j.core.convert.ConvertWith
import org.springframework.data.neo4j.core.schema.DynamicLabels
import org.springframework.data.neo4j.core.schema.Id
import org.springframework.data.neo4j.core.schema.Node
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime

@Node
@JsonIgnoreProperties(ignoreUnknown = true)
data class EntityAccessControl(

    @Id
    @JsonProperty("@id")
    @ConvertWith(converter = Neo4jUriPropertyConverter::class)
    val id: URI,

    @DynamicLabels
    @JsonProperty("@type")
    val type: List<String>,

    @JsonIgnore
    val createdAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),

    @JsonIgnore
    @LastModifiedDate
    var modifiedAt: ZonedDateTime? = null,

    val right: String,

    val specificAccessPolicy: String? = null,

    var contexts: List<String> = mutableListOf()
) {
    fun serializeCoreProperties(includeSysAttrs: Boolean): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        resultEntity[JsonLdUtils.JSONLD_ID] = id.toString()
        resultEntity[JsonLdUtils.JSONLD_TYPE] = type

        if (includeSysAttrs) {
            resultEntity[JsonLdUtils.NGSILD_CREATED_AT_PROPERTY] = mapOf(
                JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_DATE_TIME_TYPE,
                JsonLdUtils.JSONLD_VALUE_KW to createdAt.toNgsiLdFormat()
            )

            modifiedAt?.run {
                resultEntity[JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY] = mapOf(
                    JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_DATE_TIME_TYPE,
                    JsonLdUtils.JSONLD_VALUE_KW to this.toNgsiLdFormat()
                )
            }
        }

        resultEntity["right"] = mutableMapOf(
            JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_PROPERTY_TYPE.uri,
            JsonLdUtils.NGSILD_PROPERTY_VALUE to mapOf(
                JsonLdUtils.JSONLD_VALUE_KW to right
            )
        )

        if (specificAccessPolicy != null) {
            resultEntity["specificAccessPolicy"] = mutableMapOf(
                JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_PROPERTY_TYPE.uri,
                JsonLdUtils.NGSILD_PROPERTY_VALUE to mapOf(
                    JsonLdUtils.JSONLD_VALUE_KW to specificAccessPolicy
                )
            )
        }

        return resultEntity
    }
}
