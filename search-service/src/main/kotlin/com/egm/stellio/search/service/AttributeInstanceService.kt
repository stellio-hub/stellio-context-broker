package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.compactFragment
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.instanceNotFoundMessage
import io.r2dbc.postgresql.codec.Json
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

@Service
class AttributeInstanceService(
    private val databaseClient: DatabaseClient
) {

    @Transactional
    suspend fun create(attributeInstance: AttributeInstance): Either<APIException, Unit> {
        val insertStatement =
            if (attributeInstance.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT)
                """
                INSERT INTO attribute_instance 
                    (time, measured_value, value, temporal_entity_attribute, instance_id, payload)
                VALUES (:time, :measured_value, :value, :temporal_entity_attribute, :instance_id, :payload)
                ON CONFLICT (time, temporal_entity_attribute)
                DO UPDATE SET value = :value, measured_value = :measured_value, payload = :payload,
                              instance_id = :instance_id
                """.trimIndent()
            else
                """
                INSERT INTO attribute_instance_audit
                    (time, time_property, measured_value, value, temporal_entity_attribute, instance_id, payload, sub)
                VALUES
                    (:time, :time_property, :measured_value, :value, :temporal_entity_attribute, 
                        :instance_id, :payload, :sub)
                """.trimIndent()

        return databaseClient.sql(insertStatement)
            .bind("time", attributeInstance.time)
            .bind("measured_value", attributeInstance.measuredValue)
            .bind("value", attributeInstance.value)
            .bind("temporal_entity_attribute", attributeInstance.temporalEntityAttribute)
            .bind("instance_id", attributeInstance.instanceId)
            .bind("payload", Json.of(attributeInstance.payload))
            .let {
                if (attributeInstance.timeProperty != AttributeInstance.TemporalProperty.OBSERVED_AT)
                    it.bind("time_property", attributeInstance.timeProperty.toString())
                        .bind("sub", attributeInstance.sub)
                else it
            }
            .execute()
    }

    @Transactional
    suspend fun addAttributeInstance(
        temporalEntityAttributeUuid: UUID,
        attributeKey: String,
        attributeValues: Map<String, List<Any>>,
        contexts: List<String>
    ): Either<APIException, Unit> {
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

    suspend fun search(
        temporalQuery: TemporalQuery,
        temporalEntityAttribute: TemporalEntityAttribute,
        withTemporalValues: Boolean
    ): List<AttributeInstanceResult> =
        search(temporalQuery, listOf(temporalEntityAttribute), withTemporalValues)

    suspend fun search(
        temporalQuery: TemporalQuery,
        temporalEntityAttributes: List<TemporalEntityAttribute>,
        inQueryEntities: Boolean
    ): List<AttributeInstanceResult> {
        val temporalEntityAttributesIds =
            temporalEntityAttributes.joinToString(",") { "'${it.id}'" }

        // time_bucket has a default origin set to 2000-01-03
        // (see https://docs.timescale.com/api/latest/hyperfunctions/time_bucket/)
        // so we force the default origin to:
        // - timeAt if it is provided
        // - the oldest value if not (timeAt is optional if querying a temporal entity by id)
        val timestamp =
            if (temporalQuery.timeBucket != null)
                temporalQuery.timeAt ?: selectOldestDate(temporalQuery, temporalEntityAttributesIds)
            else null

        var selectQuery = composeSearchSelectStatement(temporalQuery, temporalEntityAttributes, timestamp)

        if (!inQueryEntities && temporalQuery.timeBucket == null)
            selectQuery = selectQuery.plus(", payload::TEXT")

        selectQuery =
            if (temporalQuery.timeproperty == AttributeInstance.TemporalProperty.OBSERVED_AT)
                selectQuery.plus(
                    """
                    FROM attribute_instance
                    WHERE temporal_entity_attribute IN($temporalEntityAttributesIds)
                    """
                )
            else
                selectQuery.plus(
                    """
                    FROM attribute_instance_audit
                    WHERE temporal_entity_attribute IN($temporalEntityAttributesIds)
                    AND time_property = '${temporalQuery.timeproperty.name}'
                    """
                )

        selectQuery = when (temporalQuery.timerel) {
            TemporalQuery.Timerel.BEFORE -> selectQuery.plus(" AND time < '${temporalQuery.timeAt}'")
            TemporalQuery.Timerel.AFTER -> selectQuery.plus(" AND time > '${temporalQuery.timeAt}'")
            TemporalQuery.Timerel.BETWEEN -> selectQuery.plus(
                " AND time > '${temporalQuery.timeAt}' AND time < '${temporalQuery.endTimeAt}'"
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
            .allToMappedList { rowToAttributeInstanceResult(it, temporalQuery, inQueryEntities) }
    }

    private fun composeSearchSelectStatement(
        temporalQuery: TemporalQuery,
        temporalEntityAttributes: List<TemporalEntityAttribute>,
        timestamp: ZonedDateTime?
    ) = when {
        temporalQuery.timeBucket != null ->
            """
            SELECT temporal_entity_attribute,
                   time_bucket('${temporalQuery.timeBucket}', time, TIMESTAMPTZ '${timestamp!!}') as time_bucket,
                   ${temporalQuery.aggregate}(measured_value) as value
            """.trimIndent()
        // temporal entity attributes are grouped by attribute type by calling services
        temporalEntityAttributes[0].attributeValueType == TemporalEntityAttribute.AttributeValueType.ANY &&
            temporalQuery.timeproperty == AttributeInstance.TemporalProperty.OBSERVED_AT ->
            "SELECT temporal_entity_attribute, time, value "
        temporalEntityAttributes[0].attributeValueType == TemporalEntityAttribute.AttributeValueType.ANY &&
            temporalQuery.timeproperty != AttributeInstance.TemporalProperty.OBSERVED_AT ->
            "SELECT temporal_entity_attribute, time, value, sub "
        temporalQuery.timeproperty != AttributeInstance.TemporalProperty.OBSERVED_AT ->
            "SELECT temporal_entity_attribute, time, measured_value as value, sub "
        else ->
            "SELECT temporal_entity_attribute, time, measured_value as value "
    }

    suspend fun selectOldestDate(
        temporalQuery: TemporalQuery,
        temporalEntityAttributesIds: String
    ): ZonedDateTime? {
        var selectQuery =
            """
                SELECT min(time) as first
            """.trimIndent()

        selectQuery =
            if (temporalQuery.timeproperty == AttributeInstance.TemporalProperty.OBSERVED_AT)
                selectQuery.plus(
                    """
                    FROM attribute_instance
                    WHERE temporal_entity_attribute IN($temporalEntityAttributesIds)
                    """
                )
            else
                selectQuery.plus(
                    """
                    FROM attribute_instance_audit
                    WHERE temporal_entity_attribute IN($temporalEntityAttributesIds)
                    AND time_property = '${temporalQuery.timeproperty.name}'
                    """
                )

        return databaseClient
            .sql(selectQuery)
            .oneToResult { ZonedDateTime.parse(it["first"].toString()).toInstant().atZone(ZoneOffset.UTC) }
            .orNull()
    }

    private fun rowToAttributeInstanceResult(
        row: Map<String, Any>,
        temporalQuery: TemporalQuery,
        withTemporalValues: Boolean
    ): AttributeInstanceResult {
        return if (withTemporalValues || temporalQuery.timeBucket != null)
            SimplifiedAttributeInstanceResult(
                temporalEntityAttribute = (row["temporal_entity_attribute"] as? UUID)!!,
                value = row["value"]!!,
                time = row["time_bucket"]?.let {
                    ZonedDateTime.parse(it.toString()).toInstant().atZone(ZoneOffset.UTC)
                } ?: row["time"]
                    .let { ZonedDateTime.parse(it.toString()).toInstant().atZone(ZoneOffset.UTC) }
            )
        else FullAttributeInstanceResult(
            temporalEntityAttribute = (row["temporal_entity_attribute"] as? UUID)!!,
            payload = row["payload"].let { it as String },
            sub = row["sub"] as? String
        )
    }

    suspend fun deleteEntityAttributeInstance(
        entityId: URI,
        entityAttributeName: String,
        instanceId: URI
    ): Either<APIException, Unit> {
        val deleteQuery =
            """
                DELETE FROM attribute_instance
                WHERE temporal_entity_attribute = ( 
                    SELECT id 
                    FROM temporal_entity_attribute 
                    WHERE entity_id = :entity_id 
                    AND attribute_name = :attribute_name
                )
                AND instance_id = :instance_id
            """.trimIndent()

        return databaseClient
            .sql(deleteQuery)
            .bind("entity_id", entityId)
            .bind("attribute_name", entityAttributeName)
            .bind("instance_id", instanceId)
            .executeExpected {
                if (it == 0)
                    ResourceNotFoundException(instanceNotFoundMessage(instanceId.toString())).left()
                else Unit.right()
            }
    }
}
