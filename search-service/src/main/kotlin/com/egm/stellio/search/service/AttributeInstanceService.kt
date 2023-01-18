package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import arrow.fx.coroutines.parTraverseEither
import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.instanceNotFoundMessage
import io.r2dbc.postgresql.codec.Json
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

@Service
class AttributeInstanceService(
    private val databaseClient: DatabaseClient
) {

    private val attributesInstancesTables = listOf("attribute_instance", "attribute_instance_audit")

    @Transactional
    suspend fun create(attributeInstance: AttributeInstance): Either<APIException, Unit> {
        val insertStatement =
            if (attributeInstance.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT &&
                attributeInstance.geoValue != null
            )
                """
                INSERT INTO attribute_instance 
                    (time, measured_value, value, geo_value, temporal_entity_attribute, 
                        instance_id, payload)
                VALUES 
                    (:time, :measured_value, :value, ST_GeomFromText(:geo_value), :temporal_entity_attribute, 
                        :instance_id, :payload)
                ON CONFLICT (time, temporal_entity_attribute)
                DO UPDATE SET value = :value, measured_value = :measured_value, payload = :payload,
                              instance_id = :instance_id
                """.trimIndent()
            else if (attributeInstance.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT)
                """
                INSERT INTO attribute_instance 
                    (time, measured_value, value, temporal_entity_attribute, 
                        instance_id, payload)
                VALUES 
                    (:time, :measured_value, :value, :temporal_entity_attribute, 
                        :instance_id, :payload)
                ON CONFLICT (time, temporal_entity_attribute)
                DO UPDATE SET value = :value, measured_value = :measured_value, payload = :payload                    
                """.trimIndent()
            else if (attributeInstance.geoValue != null)
                """
                INSERT INTO attribute_instance_audit
                    (time, time_property, measured_value, value, geo_value, 
                        temporal_entity_attribute, instance_id, payload, sub)
                VALUES
                    (:time, :time_property, :measured_value, :value, ST_GeomFromText(:geo_value), 
                        :temporal_entity_attribute, :instance_id, :payload, :sub)
                """.trimIndent()
            else
                """
                INSERT INTO attribute_instance_audit
                    (time, time_property, measured_value, value,
                        temporal_entity_attribute, instance_id, payload, sub)
                VALUES
                    (:time, :time_property, :measured_value, :value, 
                        :temporal_entity_attribute, :instance_id, :payload, :sub)
                """.trimIndent()

        return databaseClient.sql(insertStatement)
            .bind("time", attributeInstance.time)
            .bind("measured_value", attributeInstance.measuredValue)
            .bind("value", attributeInstance.value)
            .let {
                if (attributeInstance.geoValue != null)
                    it.bind("geo_value", attributeInstance.geoValue.value)
                else it
            }
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
        attributeName: ExpandedTerm,
        attributeValues: Map<String, List<Any>>
    ): Either<APIException, Unit> {
        val attributeValue = getPropertyValueFromMap(attributeValues, NGSILD_PROPERTY_VALUE)
            ?: throw BadRequestDataException("Attribute $attributeName has an instance without a value")
        val observedAt = getPropertyValueFromMapAsDateTime(attributeValues, NGSILD_OBSERVED_AT_PROPERTY)
            ?: throw BadRequestDataException("Attribute $attributeName has an instance without an observed date")

        val attributeInstance = AttributeInstance(
            temporalEntityAttribute = temporalEntityAttributeUuid,
            timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT,
            time = observedAt,
            value = valueToStringOrNull(attributeValue),
            measuredValue = valueToDoubleOrNull(attributeValue),
            payload = attributeValues
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
            selectQuery = selectQuery.plus(", payload")

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
        else -> {
            val valueColumn = when (temporalEntityAttributes[0].attributeValueType) {
                TemporalEntityAttribute.AttributeValueType.NUMBER -> "measured_value as value"
                TemporalEntityAttribute.AttributeValueType.GEOMETRY -> "ST_AsText(geo_value) as value"
                else -> "value"
            }
            val subColumn = when (temporalQuery.timeproperty) {
                AttributeInstance.TemporalProperty.OBSERVED_AT -> null
                else -> "sub"
            }
            "SELECT " + listOfNotNull("temporal_entity_attribute", "time", valueColumn, subColumn)
                .joinToString(",")
        }
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
            .oneToResult { toZonedDateTime(it["first"]) }
            .orNull()
    }

    private fun rowToAttributeInstanceResult(
        row: Map<String, Any>,
        temporalQuery: TemporalQuery,
        withTemporalValues: Boolean
    ): AttributeInstanceResult {
        return if (withTemporalValues || temporalQuery.timeBucket != null)
            SimplifiedAttributeInstanceResult(
                temporalEntityAttribute = toUuid(row["temporal_entity_attribute"]),
                value = row["value"]!!,
                time = toOptionalZonedDateTime(row["time_bucket"]) ?: toZonedDateTime(row["time"])
            )
        else FullAttributeInstanceResult(
            temporalEntityAttribute = toUuid(row["temporal_entity_attribute"]),
            payload = toJsonString(row["payload"]),
            time = toZonedDateTime(row["time"]),
            timeproperty = temporalQuery.timeproperty.propertyName,
            sub = row["sub"] as? String
        )
    }

    @Transactional
    suspend fun deleteInstance(
        entityId: URI,
        attributeName: ExpandedTerm,
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
            .bind("attribute_name", attributeName)
            .bind("instance_id", instanceId)
            .executeExpected {
                if (it == 0)
                    ResourceNotFoundException(instanceNotFoundMessage(instanceId.toString())).left()
                else Unit.right()
            }
    }

    @Transactional
    suspend fun deleteInstancesOfAttribute(
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI?
    ): Either<APIException, Unit> =
        attributesInstancesTables.parTraverseEither {
            val deleteQuery =
                """
                DELETE FROM $it
                WHERE temporal_entity_attribute = ( 
                    SELECT id 
                    FROM temporal_entity_attribute 
                    WHERE entity_id = :entity_id 
                    AND attribute_name = :attribute_name
                    ${datasetId.toDatasetIdFilter()}
                )
                """.trimIndent()

            databaseClient
                .sql(deleteQuery)
                .bind("entity_id", entityId)
                .bind("attribute_name", attributeName)
                .let {
                    if (datasetId != null) it.bind("dataset_id", datasetId)
                    else it
                }
                .execute()
        }.map { }

    @Transactional
    suspend fun deleteAllInstancesOfAttribute(
        entityId: URI,
        attributeName: ExpandedTerm
    ): Either<APIException, Unit> =
        attributesInstancesTables.parTraverseEither {
            val deleteQuery =
                """
                DELETE FROM $it
                WHERE temporal_entity_attribute IN ( 
                    SELECT id 
                    FROM temporal_entity_attribute 
                    WHERE entity_id = :entity_id 
                    AND attribute_name = :attribute_name
                )
                """.trimIndent()

            databaseClient
                .sql(deleteQuery)
                .bind("entity_id", entityId)
                .bind("attribute_name", attributeName)
                .execute()
        }.map { }

    @Transactional
    suspend fun deleteInstancesOfEntity(
        entityId: URI
    ): Either<APIException, Unit> =
        attributesInstancesTables.parTraverseEither {
            val deleteQuery =
                """
                DELETE FROM $it
                WHERE temporal_entity_attribute IN ( 
                    SELECT id 
                    FROM temporal_entity_attribute 
                    WHERE entity_id = :entity_id 
                )
                """.trimIndent()

            databaseClient
                .sql(deleteQuery)
                .bind("entity_id", entityId)
                .execute()
        }.map { }
}
