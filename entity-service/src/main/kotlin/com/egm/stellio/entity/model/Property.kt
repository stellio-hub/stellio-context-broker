package com.egm.stellio.entity.model

import com.egm.stellio.entity.util.NgsiLdParsingUtils
import com.fasterxml.jackson.annotation.JsonRawValue
import org.neo4j.ogm.annotation.NodeEntity
import java.time.OffsetDateTime

@NodeEntity
class Property(
    val name: String,
    var unitCode: String? = null,

    @JsonRawValue
    var value: Any,

    observedAt: OffsetDateTime? = null
) : Attribute(attributeType = "Property", observedAt = observedAt) {

    override fun serializeCoreProperties(): MutableMap<String, Any> {
        val resultEntity = super.serializeCoreProperties()

        resultEntity[NgsiLdParsingUtils.NGSILD_ENTITY_TYPE] = NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE.uri
        resultEntity[NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE] = value
        unitCode?.run {
            resultEntity[NgsiLdParsingUtils.NGSILD_UNIT_CODE_PROPERTY] = this
        }

        return resultEntity
    }

    fun updateValues(unitCode: String?, value: Any?, observedAt: OffsetDateTime?) {
        unitCode?.let {
            this.unitCode = unitCode
        }
        value?.let {
            this.value = value
        }
        observedAt?.let {
            this.observedAt = observedAt
        }
    }
}