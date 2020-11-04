package com.egm.stellio.entity.model

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

    constructor(name: String, ngsiLdPropertyInstance: NgsiLdPropertyInstance) :
        this(
            name = name,
            value = ngsiLdPropertyInstance.value,
            unitCode = ngsiLdPropertyInstance.unitCode,
            observedAt = ngsiLdPropertyInstance.observedAt,
            datasetId = ngsiLdPropertyInstance.datasetId
        )

    override fun serializeCoreProperties(includeSysAttrs: Boolean): MutableMap<String, Any> {
        val resultEntity = super.serializeCoreProperties(includeSysAttrs)
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
        val propsMap = super.nodeProperties()
        propsMap["name"] = name
        propsMap["value"] = value

        unitCode?.run {
            propsMap["unitCode"] = this
        }

        return propsMap
    }

    fun updateValues(unitCode: String?, value: Any?, observedAt: ZonedDateTime?): Property {
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
