package com.egm.stellio.entity.model

import com.egm.stellio.shared.model.NgsiLdRelationshipInstance
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import com.fasterxml.jackson.annotation.JsonProperty
import org.neo4j.ogm.annotation.Labels
import org.neo4j.ogm.annotation.NodeEntity
import java.net.URI
import java.time.ZonedDateTime

@NodeEntity
class Relationship(

    @Labels
    @JsonProperty("@type")
    val type: List<String>,

    observedAt: ZonedDateTime? = null,

    datasetId: URI? = null

) : Attribute(attributeType = "Relationship", observedAt = observedAt, datasetId = datasetId) {

    constructor(type: String, ngsiLdRelationshipInstance: NgsiLdRelationshipInstance) :
        this(
            type = listOf(type),
            observedAt = ngsiLdRelationshipInstance.observedAt,
            datasetId = ngsiLdRelationshipInstance.datasetId
        )

    fun updateValues(observedAt: ZonedDateTime?): Relationship {
        observedAt?.let {
            this.observedAt = observedAt
        }
        return this
    }

    fun updateValues(updateFragment: Map<String, List<Any>>): Relationship =
        updateValues(getPropertyValueFromMapAsDateTime(updateFragment, JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY))
}

fun String.toRelationshipTypeName(): String =
    this.extractShortTypeFromExpanded()
