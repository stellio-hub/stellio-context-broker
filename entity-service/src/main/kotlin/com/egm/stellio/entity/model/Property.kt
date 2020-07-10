package com.egm.stellio.entity.model

import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.shared.util.NgsiLdParsingUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATE_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_TIME_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_UNIT_CODE_PROPERTY
import com.fasterxml.jackson.annotation.JsonRawValue
import org.neo4j.ogm.annotation.Index
import org.neo4j.ogm.annotation.NodeEntity
import java.net.URI
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime

@NodeEntity
class Property(
    @Index
    val name: String,
    var unitCode: String? = null,

    @JsonRawValue
    var value: Any,

    observedAt: ZonedDateTime? = null,

    datasetId: URI? = null

) : Attribute(attributeType = "Property", observedAt = observedAt, datasetId = datasetId) {

    override fun serializeCoreProperties(): MutableMap<String, Any> {
        val resultEntity = super.serializeCoreProperties()
        resultEntity[NGSILD_ENTITY_TYPE] = NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE.uri
        resultEntity[NGSILD_PROPERTY_VALUE] = when (value) {
            is ZonedDateTime -> mapOf(NGSILD_ENTITY_TYPE to NGSILD_DATE_TIME_TYPE, JSONLD_VALUE_KW to value)
            is LocalDate -> mapOf(NGSILD_ENTITY_TYPE to NGSILD_DATE_TYPE, JSONLD_VALUE_KW to value)
            is LocalTime -> mapOf(NGSILD_ENTITY_TYPE to NGSILD_TIME_TYPE, JSONLD_VALUE_KW to value)
            else -> value
        }
        unitCode?.run {
            resultEntity[NGSILD_UNIT_CODE_PROPERTY] = this
        }

        return resultEntity
    }

    override fun nodeProperties(): MutableMap<String, Any> {
        val propsMap = super.nodeProperties()
        propsMap["name"] = name
        propsMap["value"] = value

        unitCode?.run {
            propsMap["unitCode"] = this
        }

        return propsMap
    }

    fun updateValues(unitCode: String?, value: Any?, observedAt: ZonedDateTime?) {
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
