package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.compactFragment
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import io.r2dbc.postgresql.codec.Json
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
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
        databaseClient.sql(
            """
            INSERT INTO attribute_instance 
                (time, measured_value, value, temporal_entity_attribute, instance_id, payload)
                VALUES (:time, :measured_value, :value, :temporal_entity_attribute, :instance_id, :payload)
            ON CONFLICT (time, temporal_entity_attribute)
                DO UPDATE SET value = :value, measured_value = :measured_value, payload = :payload
            """.trimIndent()
        )
            .bind("time", attributeInstance.time)
            .bind("measured_value", attributeInstance.measuredValue)
            .bind("value", attributeInstance.value)
            .bind("temporal_entity_attribute", attributeInstance.temporalEntityAttribute)
            .bind("instance_id", attributeInstance.instanceId)
            .bind("payload", Json.of(attributeInstance.payload))
            .fetch()
            .rowsUpdated()
            .onErrorReturn(-1)

    @Transactional
    fun addAttributeInstance(
        temporalEntityAttributeUuid: UUID,
        attributeKey: String,
        attributeValues: Map<String, List<Any>>,
        contexts: List<String>
    ): Mono<Int> {
        val attributeValue = getPropertyValueFromMap(attributeValues, NGSILD_PROPERTY_VALUE)
            ?: throw BadRequestDataException("Attribute $attributeKey has an instance without a value")
        val observedAt = getPropertyValueFromMapAsDateTime(attributeValues, NGSILD_OBSERVED_AT_PROPERTY)
            ?: throw BadRequestDataException("Attribute $attributeKey has an instance without an observed date")

        val attributeInstance = AttributeInstance(
            temporalEntityAttribute = temporalEntityAttributeUuid,
            timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT,
            time = observedAt,
            value = valueToStringOrNull(attributeValue),
            measuredValue = valueToDoubleOrNull(attributeValue),
            payload = compactFragment(attributeValues, contexts).minus(JSONLD_CONTEXT)
        )
        return create(attributeInstance)
    }

    fun search(
        temporalQuery: TemporalQuery,
        temporalEntityAttribute: TemporalEntityAttribute,
        withTemporalValues: Boolean
    ): Mono<List<AttributeInstanceResult>> =
        search(temporalQuery, listOf(temporalEntityAttribute), withTemporalValues)

    fun search(
        temporalQuery: TemporalQuery,
        temporalEntityAttributes: List<TemporalEntityAttribute>,
        withTemporalValues: Boolean
    ): Mono<List<AttributeInstanceResult>> {
        var selectQuery =
            when {
                temporalQuery.timeBucket != null ->
                    """
                        SELECT temporal_entity_attribute,
                               time_bucket('${temporalQuery.timeBucket}', time) as time_bucket,
                               ${temporalQuery.aggregate}(measured_value) as value
                    """.trimIndent()
                // temporal entity attributes are grouped by attribute type by calling services
                temporalEntityAttributes[0].attributeValueType == TemporalEntityAttribute.AttributeValueType.ANY ->
                    """
                        SELECT temporal_entity_attribute, time, value
                    """.trimIndent()
                else ->
                    """
                        SELECT temporal_entity_attribute, time, measured_value as value
                    """.trimIndent()
            }

        if (!withTemporalValues && temporalQuery.timeBucket == null)
            selectQuery = selectQuery.plus(", payload::TEXT")

        val temporalEntityAttributesIds =
            temporalEntityAttributes.joinToString(",") { "'${it.id}'" }

        selectQuery = selectQuery.plus(
            """
                FROM attribute_instance
                WHERE temporal_entity_attribute IN($temporalEntityAttributesIds)
            """
        )

        selectQuery = when (temporalQuery.timerel) {
            TemporalQuery.Timerel.BEFORE -> selectQuery.plus(" AND time < '${temporalQuery.time}'")
            TemporalQuery.Timerel.AFTER -> selectQuery.plus(" AND time > '${temporalQuery.time}'")
            TemporalQuery.Timerel.BETWEEN -> selectQuery.plus(
                " AND time > '${temporalQuery.time}' AND time < '${temporalQuery.endTime}'"
            )
            else -> selectQuery
        }

        selectQuery = if (temporalQuery.timeBucket != null)
            selectQuery.plus(" GROUP BY temporal_entity_attribute, time_bucket ORDER BY time_bucket DESC")
        else
            selectQuery.plus(" ORDER BY time DESC")

        if (temporalQuery.lastN != null)
            selectQuery = selectQuery.plus(" LIMIT ${temporalQuery.lastN}")

        return databaseClient.sql(selectQuery)
            .fetch()
            .all()
            .map {
                rowToAttributeInstanceResult(it, temporalQuery, withTemporalValues)
            }
            .collectList()
    }

    fun deleteAttributeInstancesOfEntity(entityId: URI): Mono<Int> =
        databaseClient.sql(
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
        databaseClient.sql(
            """
            DELETE FROM attribute_instance WHERE temporal_entity_attribute IN (
                SELECT id FROM temporal_entity_attribute WHERE 
                    entity_id = :entity_id
                    ${if (datasetId != null) "AND dataset_id = :dataset_id" else "AND dataset_id IS NULL"}
                    AND attribute_name = :attribute_name
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

    fun deleteAllAttributeInstancesOfTemporalAttribute(entityId: URI, attributeName: String): Mono<Int> =
        databaseClient.sql(
            """
            DELETE FROM attribute_instance WHERE temporal_entity_attribute IN (
                SELECT id FROM temporal_entity_attribute WHERE 
                    entity_id = :entity_id AND
                    attribute_name = :attribute_name
            )
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("attribute_name", attributeName)
            .fetch()
            .rowsUpdated()

    private fun rowToAttributeInstanceResult(
        row: Map<String, Any>,
        temporalQuery: TemporalQuery,
        withTemporalValues: Boolean
    ): AttributeInstanceResult {
        return if (withTemporalValues || temporalQuery.timeBucket != null)
            SimplifiedAttributeInstanceResult(
                temporalEntityAttribute = (row["temporal_entity_attribute"] as UUID?)!!,
                value = row["value"]!!,
                observedAt = row["time_bucket"]?.let {
                    ZonedDateTime.parse(it.toString()).toInstant().atZone(ZoneOffset.UTC)
                } ?: row["time"]
                    .let { ZonedDateTime.parse(it.toString()).toInstant().atZone(ZoneOffset.UTC) }
            )
        else FullAttributeInstanceResult(
            temporalEntityAttribute = (row["temporal_entity_attribute"] as UUID?)!!,
            payload = row["payload"].let { it as String }
        )
    }
}
