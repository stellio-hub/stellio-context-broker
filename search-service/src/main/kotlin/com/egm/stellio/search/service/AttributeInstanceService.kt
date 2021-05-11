package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils.EGM_OBSERVED_BY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.extractAttributeInstanceFromCompactedEntity
import io.r2dbc.postgresql.codec.Json
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Mono
import java.net.URI
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

@Service
class AttributeInstanceService(
    private val databaseClient: DatabaseClient
) {

    @Transactional
    fun create(attributeInstance: AttributeInstance): Mono<Int> =
        databaseClient.execute(
            """
            INSERT INTO attribute_instance 
                (observed_at, measured_value, value, temporal_entity_attribute, instance_id, payload)
                VALUES (:observed_at, :measured_value, :value, :temporal_entity_attribute, :instance_id, :payload)
            """
        )
            .bind("observed_at", attributeInstance.observedAt)
            .bind("measured_value", attributeInstance.measuredValue)
            .bind("value", attributeInstance.value)
            .bind("temporal_entity_attribute", attributeInstance.temporalEntityAttribute)
            .bind("instance_id", attributeInstance.instanceId)
            .bind("payload", Json.of(attributeInstance.payload))
            .fetch()
            .rowsUpdated()
            .onErrorReturn(-1)

    // TODO not totally compatible with the specification
    // it should accept an array of attribute instances
    fun addAttributeInstances(
        temporalEntityAttributeUuid: UUID,
        attributeKey: String,
        attributeValues: Map<String, List<Any>>,
        parsedPayload: Map<String, Any>
    ): Mono<Int> {
        val attributeValue = getPropertyValueFromMap(attributeValues, NGSILD_PROPERTY_VALUE)
            ?: throw BadRequestDataException("Value cannot be null")

        val attributeInstance = AttributeInstance(
            temporalEntityAttribute = temporalEntityAttributeUuid,
            observedAt = getPropertyValueFromMapAsDateTime(attributeValues, EGM_OBSERVED_BY)!!,
            value = valueToStringOrNull(attributeValue),
            measuredValue = valueToDoubleOrNull(attributeValue),
            payload =
                extractAttributeInstanceFromCompactedEntity(
                    parsedPayload,
                    attributeKey,
                    null
                )
        )
        return create(attributeInstance)
    }

    fun search(
        temporalQuery: TemporalQuery,
        temporalEntityAttribute: TemporalEntityAttribute,
        withTemporalValues: Boolean
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

        if (!withTemporalValues && temporalQuery.timeBucket == null)
            selectQuery = selectQuery.plus(", payload::TEXT")

        selectQuery = selectQuery.plus(
            """
                FROM attribute_instance
                WHERE temporal_entity_attribute = '${temporalEntityAttribute.id}' 
            """
        )

        selectQuery = when (temporalQuery.timerel) {
            TemporalQuery.Timerel.BEFORE -> selectQuery.plus(" AND observed_at < '${temporalQuery.time}'")
            TemporalQuery.Timerel.AFTER -> selectQuery.plus(" AND observed_at > '${temporalQuery.time}'")
            TemporalQuery.Timerel.BETWEEN -> selectQuery.plus(
                " AND observed_at > '${temporalQuery.time}' AND observed_at < '${temporalQuery.endTime}'"
            )
            else -> selectQuery
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
                rowToAttributeInstanceResult(it, temporalQuery, withTemporalValues)
            }
            .collectList()
    }

    fun deleteAttributeInstancesOfEntity(entityId: URI): Mono<Int> =
        databaseClient.execute(
            """
            DELETE FROM attribute_instance WHERE temporal_entity_attribute IN (
                SELECT id FROM temporal_entity_attribute WHERE entity_id = :entity_id
            )
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .fetch()
            .rowsUpdated()

    fun deleteAttributeInstancesOfTemporalAttribute(entityId: URI, attributeName: String, datasetId: URI?): Mono<Int> =
        databaseClient.execute(
            """
            DELETE FROM attribute_instance WHERE temporal_entity_attribute IN (
                SELECT id FROM temporal_entity_attribute WHERE 
                    entity_id = :entity_id AND
                    ${if (datasetId != null) "AND dataset_id = :dataset_id" else ""}
                    attribute_name = :attribute_name
            )
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("attribute_name", attributeName)
            .let {
                if (datasetId != null) it.bind("dataset_id", datasetId)
                else it
            }
            .fetch()
            .rowsUpdated()

    private fun rowToAttributeInstanceResult(
        row: Map<String, Any>,
        temporalQuery: TemporalQuery,
        withTemporalValues: Boolean
    ): AttributeInstanceResult {
        return if (withTemporalValues || temporalQuery.timeBucket != null)
            SimplifiedAttributeInstanceResult(
                value = row["value"]!!,
                observedAt =
                    row["time_bucket"]?.let { ZonedDateTime.parse(it.toString()).toInstant().atZone(ZoneOffset.UTC) }
                        ?: row["observed_at"]
                            .let { ZonedDateTime.parse(it.toString()).toInstant().atZone(ZoneOffset.UTC) }
            )
        else FullAttributeInstanceResult(row["payload"].let { it as String })
    }
}
