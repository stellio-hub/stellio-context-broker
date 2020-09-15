package com.egm.stellio.entity.model

import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.fasterxml.jackson.annotation.JsonIgnore
import org.neo4j.ogm.annotation.Id
import org.neo4j.ogm.annotation.NodeEntity
import org.neo4j.ogm.annotation.Relationship
import org.neo4j.ogm.annotation.Transient
import org.neo4j.ogm.annotation.typeconversion.Convert
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

@NodeEntity
open class Attribute(

    @Transient
    val attributeType: String,

    var observedAt: ZonedDateTime? = null,

    @JsonIgnore
    val createdAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),

    @JsonIgnore
    var modifiedAt: ZonedDateTime? = null,

    @Convert(UriConverter::class)
    var datasetId: URI? = null,

    @Relationship(type = "HAS_VALUE")
    val properties: MutableList<Property> = mutableListOf(),

    @Relationship(type = "HAS_OBJECT")
    val relationships: MutableList<com.egm.stellio.entity.model.Relationship> = mutableListOf()

) {
    @Id
    @Convert(UriConverter::class)
    val id: URI = URI.create("urn:ngsi-ld:$attributeType:${UUID.randomUUID()}")

    open fun serializeCoreProperties(includeSysAttrs: Boolean): MutableMap<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
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
        observedAt?.run {
            resultEntity[NGSILD_OBSERVED_AT_PROPERTY] = mapOf(
                JSONLD_TYPE to NGSILD_DATE_TIME_TYPE,
                JSONLD_VALUE_KW to this
            )
        }

        datasetId?.run {
            resultEntity[NGSILD_DATASET_ID_PROPERTY] = mapOf(
                JSONLD_ID to this.toString()
            )
        }

        return resultEntity
    }

    /**
     * Return a map of the properties to store with the Property node in neo4j
     */
    open fun nodeProperties(): MutableMap<String, Any> {
        val nodeProperties = mutableMapOf(
            "id" to id,
            "createdAt" to createdAt
        )

        modifiedAt?.run {
            nodeProperties["modifiedAt"] = this
        }

        observedAt?.run {
            nodeProperties["observedAt"] = this
        }

        datasetId?.run {
            nodeProperties["datasetId"] = this
        }

        return nodeProperties
    }
}
