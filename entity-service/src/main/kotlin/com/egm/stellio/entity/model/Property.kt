package com.egm.stellio.entity.model

import com.egm.stellio.entity.config.Neo4jUriPropertyConverter
import com.egm.stellio.entity.config.Neo4jValuePropertyConverter
import com.egm.stellio.shared.model.NgsiLdPropertyInstance
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_UNIT_CODE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsString
import com.egm.stellio.shared.util.toNgsiLdFormat
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonRawValue
import org.springframework.data.annotation.LastModifiedDate
import org.springframework.data.neo4j.core.convert.ConvertWith
import org.springframework.data.neo4j.core.schema.Id
import org.springframework.data.neo4j.core.schema.Node
import org.springframework.data.neo4j.core.schema.Relationship
import java.net.URI
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

@Node
data class Property(
    @Id
    @ConvertWith(converter = Neo4jUriPropertyConverter::class)
    val id: URI = "urn:ngsi-ld:Property:${UUID.randomUUID()}".toUri(),

    var observedAt: ZonedDateTime? = null,

    @JsonIgnore
    val createdAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),

    @JsonIgnore
    @LastModifiedDate
    val modifiedAt: ZonedDateTime? = null,

    @ConvertWith(converter = Neo4jUriPropertyConverter::class)
    val datasetId: URI? = null,

    @Relationship(type = "HAS_VALUE")
    val properties: MutableList<Property> = mutableListOf(),

    @Relationship(type = "HAS_OBJECT")
    val relationships: MutableList<com.egm.stellio.entity.model.Relationship> = mutableListOf(),

    val name: String,

    var unitCode: String? = null,

    @JsonRawValue
    @ConvertWith(converter = Neo4jValuePropertyConverter::class)
    var value: Any
) : Attribute {

    constructor(name: String, ngsiLdPropertyInstance: NgsiLdPropertyInstance) :
        this(
            name = name,
            value = ngsiLdPropertyInstance.value,
            unitCode = ngsiLdPropertyInstance.unitCode,
            observedAt = ngsiLdPropertyInstance.observedAt,
            datasetId = ngsiLdPropertyInstance.datasetId
        )

    override fun serializeCoreProperties(includeSysAttrs: Boolean): MutableMap<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        if (includeSysAttrs) {
            resultEntity[JsonLdUtils.NGSILD_CREATED_AT_PROPERTY] = mapOf(
                JSONLD_TYPE to NGSILD_DATE_TIME_TYPE,
                JSONLD_VALUE_KW to createdAt.toNgsiLdFormat()
            )

            modifiedAt?.run {
                resultEntity[JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY] = mapOf(
                    JSONLD_TYPE to NGSILD_DATE_TIME_TYPE,
                    JSONLD_VALUE_KW to this.toNgsiLdFormat()
                )
            }
        }
        observedAt?.run {
            resultEntity[JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY] = mapOf(
                JSONLD_TYPE to NGSILD_DATE_TIME_TYPE,
                JSONLD_VALUE_KW to this.toNgsiLdFormat()
            )
        }

        datasetId?.run {
            resultEntity[JsonLdUtils.NGSILD_DATASET_ID_PROPERTY] = mapOf(
                JsonLdUtils.JSONLD_ID to this.toString()
            )
        }
        resultEntity[JSONLD_TYPE] = JsonLdUtils.NGSILD_PROPERTY_TYPE.uri
        resultEntity[NGSILD_PROPERTY_VALUE] = when (value) {
            is ZonedDateTime -> mapOf(JSONLD_TYPE to NGSILD_DATE_TIME_TYPE, JSONLD_VALUE_KW to value)
            is LocalDate -> mapOf(JSONLD_TYPE to NGSILD_DATE_TYPE, JSONLD_VALUE_KW to value)
            is LocalTime -> mapOf(JSONLD_TYPE to NGSILD_TIME_TYPE, JSONLD_VALUE_KW to value)
            else -> value
        }
        unitCode?.run {
            resultEntity[NGSILD_UNIT_CODE_PROPERTY] = this
        }

        return resultEntity
    }

    override fun nodeProperties(): MutableMap<String, Any> {
        val nodeProperties = mutableMapOf<String, Any>(
            "id" to id.toString(),
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

        nodeProperties["name"] = name
        nodeProperties["value"] = value

        unitCode?.run {
            nodeProperties["unitCode"] = this
        }

        return nodeProperties
    }

    override fun id(): URI = id

    private fun updateValues(unitCode: String?, value: Any?, observedAt: ZonedDateTime?): Property {
        unitCode?.let {
            this.unitCode = unitCode
        }
        value?.let {
            this.value = value
        }
        observedAt?.let {
            this.observedAt = observedAt
        }

        return this
    }

    fun updateValues(updateFragment: Map<String, List<Any>>): Property =
        updateValues(
            getPropertyValueFromMapAsString(updateFragment, NGSILD_UNIT_CODE_PROPERTY),
            getPropertyValueFromMap(updateFragment, NGSILD_PROPERTY_VALUE),
            getPropertyValueFromMapAsDateTime(updateFragment, JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY)
        )
}
