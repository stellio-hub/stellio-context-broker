package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.AttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils.EGM_OBSERVED_BY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.toUri
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

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
    fun addAttributeInstances(
        temporalEntityAttributeUuid: UUID,
        attributeKey: String,
        attributeValues: Map<String, List<Any>>
    ): Mono<Int> {
        val attributeValue = getPropertyValueFromMap(attributeValues, NGSILD_PROPERTY_VALUE)
            ?: throw BadRequestDataException("Value cannot be null")

        val attributeInstance = AttributeInstance(
            temporalEntityAttribute = temporalEntityAttributeUuid,
            observedAt = getPropertyValueFromMapAsDateTime(attributeValues, EGM_OBSERVED_BY)!!,
            value = valueToStringOrNull(attributeValue),
            measuredValue = valueToDoubleOrNull(attributeValue)
        )
        return create(attributeInstance)
    }

    fun search(
        temporalQuery: TemporalQuery,
        temporalEntityAttribute: TemporalEntityAttribute
    ): Mono<List<AttributeInstanceResult>> {
        var selectQuery =
            when {
                temporalQuery.timeBucket != null ->
                    """
                        SELECT time_bucket('${temporalQuery.timeBucket}', observed_at) as time_bucket, 
                               ${temporalQuery.aggregate}(measured_value) as value
                    """.trimIndent()
                temporalEntityAttribute.attributeValueType == TemporalEntityAttribute.AttributeValueType.ANY ->
                    """
                        SELECT observed_at, value, instance_id
                    """.trimIndent()
                else ->
                    """
                        SELECT observed_at, measured_value as value, instance_id
                    """.trimIndent()
            }

        selectQuery = selectQuery.plus(
            """
                FROM attribute_instance
                WHERE temporal_entity_attribute = '${temporalEntityAttribute.id}' 
            """
        )

        selectQuery = when (temporalQuery.timerel) {
            TemporalQuery.Timerel.BEFORE -> selectQuery.plus(" AND observed_at < '${temporalQuery.time}'")
            TemporalQuery.Timerel.AFTER -> selectQuery.plus(" AND observed_at > '${temporalQuery.time}'")
            else -> selectQuery.plus(
                " AND observed_at > '${temporalQuery.time}' AND observed_at < '${temporalQuery.endTime}'"
            )
        }

        selectQuery = if (temporalQuery.timeBucket != null)
            selectQuery.plus(" GROUP BY time_bucket ORDER BY time_bucket DESC")
        else
            selectQuery.plus(" ORDER BY observed_at DESC")

        if (temporalQuery.lastN != null)
            selectQuery = selectQuery.plus(" LIMIT ${temporalQuery.lastN}")

        return databaseClient.execute(selectQuery)
            .fetch()
            .all()
            .map {
                rowToAttributeInstanceResult(it, temporalEntityAttribute)
            }
            .collectList()
    }

    private fun rowToAttributeInstanceResult(
        row: Map<String, Any>,
        temporalEntityAttribute: TemporalEntityAttribute
    ): AttributeInstanceResult {
        return AttributeInstanceResult(
            attributeName = temporalEntityAttribute.attributeName,
            instanceId = row["instance_id"]?.let { (it as String).toUri() },
            datasetId = temporalEntityAttribute.datasetId,
            value = row["value"]!!,
            observedAt =
                row["time_bucket"]?.let { ZonedDateTime.parse(it.toString()).toInstant().atZone(ZoneOffset.UTC) }
                    ?: row["observed_at"].let { ZonedDateTime.parse(it.toString()).toInstant().atZone(ZoneOffset.UTC) }
        )
    }
}
