package com.egm.stellio.entity.model

import com.egm.stellio.entity.config.Neo4jUriPropertyConverter
import com.egm.stellio.shared.model.NgsiLdRelationshipInstance
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsString
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import com.egm.stellio.shared.util.toNgsiLdFormat
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.annotation.JsonIgnore
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
import java.util.UUID

@Node
data class Relationship(

    @Id
    @ConvertWith(converter = Neo4jUriPropertyConverter::class)
    val id: URI = "urn:ngsi-ld:Relationship:${UUID.randomUUID()}".toUri(),

    // keep a copy of the target object URI to avoid unnecessary DB calls to retrieve it
    var objectId: URI,

    var observedAt: ZonedDateTime? = null,

    @JsonIgnore
    val createdAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),

    @JsonIgnore
    @LastModifiedDate
    val modifiedAt: ZonedDateTime? = null,

    @ConvertWith(converter = Neo4jUriPropertyConverter::class)
    val datasetId: URI? = null,

    @org.springframework.data.neo4j.core.schema.Relationship(type = "HAS_VALUE")
    val properties: MutableList<Property> = mutableListOf(),

    @org.springframework.data.neo4j.core.schema.Relationship(type = "HAS_OBJECT")
    val relationships: MutableList<Relationship> = mutableListOf(),

    @DynamicLabels
    @JsonProperty("@type")
    val type: List<String>,

) : Attribute {

    constructor(type: String, ngsiLdRelationshipInstance: NgsiLdRelationshipInstance) :
        this(
            objectId = ngsiLdRelationshipInstance.objectId,
            type = listOf(type),
            observedAt = ngsiLdRelationshipInstance.observedAt,
            datasetId = ngsiLdRelationshipInstance.datasetId
        )

    override fun serializeCoreProperties(includeSysAttrs: Boolean): MutableMap<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
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
        observedAt?.run {
            resultEntity[JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY] = mapOf(
                JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_DATE_TIME_TYPE,
                JsonLdUtils.JSONLD_VALUE_KW to this.toNgsiLdFormat()
            )
        }

        datasetId?.run {
            resultEntity[JsonLdUtils.NGSILD_DATASET_ID_PROPERTY] = mapOf(
                JsonLdUtils.JSONLD_ID to this.toString()
            )
        }

        resultEntity[NGSILD_RELATIONSHIP_HAS_OBJECT] = mapOf(
            JsonLdUtils.JSONLD_ID to objectId.toString()
        )
        resultEntity[JsonLdUtils.JSONLD_TYPE] = JsonLdUtils.NGSILD_RELATIONSHIP_TYPE.uri

        return resultEntity
    }

    override fun nodeProperties(): MutableMap<String, Any> {
        val nodeProperties = mutableMapOf<String, Any>(
            "id" to id.toString(),
            "objectId" to objectId.toString(),
            "createdAt" to createdAt
        )

        modifiedAt?.run {
            nodeProperties["modifiedAt"] = this
        }

        observedAt?.run {
            nodeProperties["observedAt"] = this
        }

        datasetId?.run {
            nodeProperties["datasetId"] = this.toString()
        }

        return nodeProperties
    }

    override fun id(): URI = id

    private fun updateValues(objectId: URI?, observedAt: ZonedDateTime?): Relationship {
        objectId?.let {
            this.objectId = it
        }
        observedAt?.let {
            this.observedAt = observedAt
        }
        return this
    }

    fun updateValues(updateFragment: Map<String, List<Any>>): Relationship =
        updateValues(
            getPropertyValueFromMapAsString(updateFragment, NGSILD_RELATIONSHIP_HAS_OBJECT)?.toUri(),
            getPropertyValueFromMapAsDateTime(updateFragment, JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY)
        )

    // neo4j forces us to have a list but we know we have only one dynamic label
    fun relationshipType(): String =
        type.first()
}

fun String.toRelationshipTypeName(): String =
    this.extractShortTypeFromExpanded()
