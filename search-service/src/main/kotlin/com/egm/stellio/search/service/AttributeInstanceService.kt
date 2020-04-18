package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.util.NgsiLdParsingUtils.EGM_OBSERVED_BY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.NgsiLdParsingUtils.getPropertyValueFromMapAsDateTime
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class AttributeInstanceService(
    private val databaseClient: DatabaseClient
) {

    fun create(attributeInstance: AttributeInstance): Mono<Int> {
        return databaseClient.insert()
            .into(AttributeInstance::class.java)
            .using(attributeInstance)
            .fetch()
            .rowsUpdated()
    }

    // TODO not totally compatible with the specification
    // it should accept an array of attribute instances
    fun addAttributeInstances(temporalEntityAttribute: TemporalEntityAttribute, attributeKey: String, attributeValues: Map<String, List<Any>>): Mono<Int> {
        val attributeValue = getPropertyValueFromMap(attributeValues, NGSILD_PROPERTY_VALUE)!!
        val attributeInstance = AttributeInstance(
            temporalEntityAttribute = temporalEntityAttribute.id,
            observedAt = getPropertyValueFromMapAsDateTime(attributeValues, EGM_OBSERVED_BY)!!,
            value = valueToStringOrNull(attributeValue),
            measuredValue = valueToDoubleOrNull(attributeValue)
        )
        return create(attributeInstance)
    }

    fun search(temporalQuery: TemporalQuery, temporalEntityAttribute: TemporalEntityAttribute): Mono<List<Map<String, Any>>> {

        var selectQuery =
            when {
                temporalQuery.timeBucket != null ->
                    """
                        SELECT time_bucket('${temporalQuery.timeBucket}', observed_at) as time_bucket, 
                               ${temporalQuery.aggregate}(measured_value) as value
                    """.trimIndent()
                temporalEntityAttribute.attributeValueType == TemporalEntityAttribute.AttributeValueType.ANY ->
                    """
                        SELECT observed_at, value
                    """.trimIndent()
                else ->
                    """
                        SELECT observed_at, measured_value as value
                    """.trimIndent()
            }

        selectQuery = selectQuery.plus(
            """
                FROM attribute_instance
                WHERE temporal_entity_attribute = '${temporalEntityAttribute.id}' 
            """)

        selectQuery = when (temporalQuery.timerel) {
            TemporalQuery.Timerel.BEFORE -> selectQuery.plus(" AND observed_at < '${temporalQuery.time}'")
            TemporalQuery.Timerel.AFTER -> selectQuery.plus(" AND observed_at > '${temporalQuery.time}'")
            else -> selectQuery.plus(" AND observed_at > '${temporalQuery.time}' AND observed_at < '${temporalQuery.endTime}'")
        }

        if (temporalQuery.timeBucket != null)
            selectQuery = selectQuery.plus(" GROUP BY time_bucket")

        return databaseClient.execute(selectQuery)
            .fetch()
            .all()
            .map {
                it.plus(Pair("attribute_name", temporalEntityAttribute.attributeName))
            }
            .collectList()
    }
}
