package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.left
import arrow.core.right
import arrow.fx.coroutines.parTraverseEither
import com.egm.stellio.search.model.*
import com.egm.stellio.search.model.AggregatedAttributeInstanceResult.AggregateResult
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.INCONSISTENT_VALUES_IN_AGGREGATION_MESSAGE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.attributeOrInstanceNotFoundMessage
import io.r2dbc.postgresql.codec.Json
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.Duration
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
                              instance_id = :instance_id, geo_value = ST_GeomFromText(:geo_value)
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
                DO UPDATE SET value = :value, measured_value = :measured_value, payload = :payload,
                              instance_id = :instance_id                 
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
            .bind("payload", attributeInstance.payload)
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
            ?: return BadRequestDataException("Attribute $attributeName has an instance without a value").left()
        val observedAt = getPropertyValueFromMapAsDateTime(attributeValues, NGSILD_OBSERVED_AT_PROPERTY)
            ?: return BadRequestDataException("Attribute $attributeName has an instance without an observed date")
                .left()

        return create(
            AttributeInstance(
                temporalEntityAttribute = temporalEntityAttributeUuid,
                timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT,
                time = observedAt,
                value = valueToStringOrNull(attributeValue),
                measuredValue = valueToDoubleOrNull(attributeValue),
                payload = attributeValues
            )
        )
    }

    suspend fun search(
        temporalEntitiesQuery: TemporalEntitiesQuery,
        temporalEntityAttribute: TemporalEntityAttribute
    ): Either<APIException, List<AttributeInstanceResult>> =
        search(temporalEntitiesQuery, listOf(temporalEntityAttribute))

    suspend fun search(
        temporalEntitiesQuery: TemporalEntitiesQuery,
        temporalEntityAttributes: List<TemporalEntityAttribute>
    ): Either<APIException, List<AttributeInstanceResult>> {
        val temporalQuery = temporalEntitiesQuery.temporalQuery
        val sqlQueryBuilder = StringBuilder()

        // time_bucket has a default origin set to 2000-01-03
        // (see https://docs.timescale.com/api/latest/hyperfunctions/time_bucket/)
        // so we force the default origin to:
        // - timeAt if it is provided
        // - the oldest value if not (timeAt is optional if querying a temporal entity by id)
        val origin =
            if (temporalEntitiesQuery.withAggregatedValues)
                temporalQuery.timeAt ?: selectOldestDate(temporalQuery, temporalEntityAttributes)
            else null

        sqlQueryBuilder.append(composeSearchSelectStatement(temporalQuery, temporalEntityAttributes, origin))

        if (!temporalEntitiesQuery.withTemporalValues && !temporalEntitiesQuery.withAggregatedValues)
            sqlQueryBuilder.append(", payload")

        if (temporalQuery.timeproperty == AttributeInstance.TemporalProperty.OBSERVED_AT)
            sqlQueryBuilder.append(
                """
                FROM attribute_instance
                WHERE temporal_entity_attribute = teas.id
                """
            )
        else
            sqlQueryBuilder.append(
                """
                FROM attribute_instance_audit
                WHERE temporal_entity_attribute = teas.id
                AND time_property = '${temporalQuery.timeproperty.name}'
                """
            )

        when (temporalQuery.timerel) {
            TemporalQuery.Timerel.BEFORE -> sqlQueryBuilder.append(" AND time < '${temporalQuery.timeAt}'")
            TemporalQuery.Timerel.AFTER -> sqlQueryBuilder.append(" AND time > '${temporalQuery.timeAt}'")
            TemporalQuery.Timerel.BETWEEN -> sqlQueryBuilder.append(
                " AND time > '${temporalQuery.timeAt}' AND time < '${temporalQuery.endTimeAt}'"
            )
            null -> Unit
        }

        if (temporalEntitiesQuery.withAggregatedValues)
            sqlQueryBuilder.append(" GROUP BY temporal_entity_attribute, origin")
        else if (temporalQuery.lastN != null)
        // in order to get last instances, need to order by time desc
        // final ascending ordering of instances is done in query service
            sqlQueryBuilder.append(" ORDER BY time DESC LIMIT ${temporalQuery.lastN}")

        val finalTemporalQuery = composeFinalTemporalQuery(temporalEntityAttributes, sqlQueryBuilder.toString())

        return databaseClient.sql(finalTemporalQuery)
            .runCatching {
                this.allToMappedList { rowToAttributeInstanceResult(it, temporalEntitiesQuery) }
            }.fold(
                { it.right() },
                { OperationNotSupportedException(INCONSISTENT_VALUES_IN_AGGREGATION_MESSAGE).left() }
            )
    }

    private fun composeFinalTemporalQuery(
        temporalEntityAttributes: List<TemporalEntityAttribute>,
        aiLateralQuery: String
    ): String {
        val temporalEntityAttributesIds = temporalEntityAttributes.joinToString(",") { "('${it.id}'::uuid)" }

        return """
        SELECT ai_limited.*
        FROM (
            SELECT distinct(id)
            FROM (VALUES $temporalEntityAttributesIds) as t (id)
        ) teas
        JOIN LATERAL (
            $aiLateralQuery
        ) ai_limited ON true;
        """.trimIndent()
    }

    private fun composeSearchSelectStatement(
        temporalQuery: TemporalQuery,
        temporalEntityAttributes: List<TemporalEntityAttribute>,
        origin: ZonedDateTime?
    ) = when {
        temporalQuery.aggrPeriodDuration != null -> {
            val allAggregates = temporalQuery.aggrMethods?.joinToString(",") {
                val sqlAggregateExpression =
                    aggrMethodToSqlAggregate(it, temporalEntityAttributes[0].attributeValueType)
                "$sqlAggregateExpression as ${it.method}_value"
            }
            """
            SELECT temporal_entity_attribute,
               time_bucket('${temporalQuery.aggrPeriodDuration}', time, TIMESTAMPTZ '${origin!!}') as origin,
               $allAggregates
            """.trimIndent()
        }
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
        temporalEntityAttributes: List<TemporalEntityAttribute>
    ): ZonedDateTime? {
        val temporalEntityAttributesIds = temporalEntityAttributes.joinToString(",") { "'${it.id}'" }
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
            .getOrNull()
    }

    private fun rowToAttributeInstanceResult(
        row: Map<String, Any>,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): AttributeInstanceResult {
        return if (temporalEntitiesQuery.withAggregatedValues) {
            val startDateTime = toZonedDateTime(row["origin"])
            val endDateTime =
                startDateTime.plus(Duration.parse(temporalEntitiesQuery.temporalQuery.aggrPeriodDuration!!))
            // in a row, there is the result for each requested aggregation method
            val values = temporalEntitiesQuery.temporalQuery.aggrMethods!!.map {
                val value = row["${it.method}_value"] ?: ""
                AggregateResult(it, value, startDateTime, endDateTime)
            }
            AggregatedAttributeInstanceResult(
                temporalEntityAttribute = toUuid(row["temporal_entity_attribute"]),
                values = values
            )
        } else if (temporalEntitiesQuery.withTemporalValues)
            SimplifiedAttributeInstanceResult(
                temporalEntityAttribute = toUuid(row["temporal_entity_attribute"]),
                // the type of the value of a property may have changed in the history (e.g., from number to string)
                // in this case, just display an empty value (something happened, but we can't display it)
                value = row["value"] ?: "",
                time = toZonedDateTime(row["time"])
            )
        else FullAttributeInstanceResult(
            temporalEntityAttribute = toUuid(row["temporal_entity_attribute"]),
            payload = toJsonString(row["payload"]),
            time = toZonedDateTime(row["time"]),
            timeproperty = temporalEntitiesQuery.temporalQuery.timeproperty.propertyName,
            sub = row["sub"] as? String
        )
    }

    @Transactional
    suspend fun modifyAttributeInstance(
        entityId: URI,
        attributeName: ExpandedTerm,
        instanceId: URI,
        expandedAttributeInstances: ExpandedAttributeInstances
    ): Either<APIException, Unit> = either {
        val teaUUID = retrieveTeaUUID(entityId, attributeName, instanceId).bind()
        val ngsiLdAttribute = parseAttributeInstancesToNgsiLdAttribute(attributeName, expandedAttributeInstances)
        val ngsiLdAttributeInstance = ngsiLdAttribute.getAttributeInstances()[0]
        val attributeMetadata = ngsiLdAttributeInstance.toTemporalAttributeMetadata().bind()

        deleteInstance(entityId, attributeName, instanceId).bind()
        create(
            AttributeInstance(
                temporalEntityAttribute = teaUUID,
                time = attributeMetadata.observedAt!!,
                modifiedAt = ngsiLdDateTime(),
                instanceId = instanceId,
                payload = expandedAttributeInstances.first(),
                measuredValue = attributeMetadata.measuredValue,
                value = attributeMetadata.value,
                timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
            )
        ).bind()
    }

    private suspend fun retrieveTeaUUID(
        entityId: URI,
        attributeName: ExpandedTerm,
        instanceId: URI
    ): Either<APIException, UUID> {
        val selectedQuery =
            """
            SELECT temporal_entity_attribute
            FROM attribute_instance
            WHERE temporal_entity_attribute = any( 
                SELECT id 
                FROM temporal_entity_attribute 
                WHERE entity_id = :entity_id 
                AND attribute_name = :attribute_name
            )
            AND instance_id = :instance_id
            """.trimIndent()

        return databaseClient
            .sql(selectedQuery)
            .bind("entity_id", entityId)
            .bind("attribute_name", attributeName)
            .bind("instance_id", instanceId)
            .oneToResult(
                ResourceNotFoundException(
                    attributeOrInstanceNotFoundMessage(attributeName, instanceId.toString())
                )
            ) {
                it["temporal_entity_attribute"] as UUID
            }
    }

    @Transactional
    suspend fun updateAttributeInstancePayload(
        entityId: URI,
        attributeName: ExpandedTerm,
        instanceId: URI,
        measuredValue: Double,
        payload: Json
    ): Either<APIException, Unit> {
        val updateQuery =
            """
            UPDATE attribute_instance_audit
            SET payload = :payload,
                measured_value = :measured_value
            WHERE temporal_entity_attribute = any( 
                SELECT id 
                FROM temporal_entity_attribute 
                WHERE entity_id = :entity_id 
                AND attribute_name = :attribute_name
            )
            AND instance_id = :instance_id
            """.trimIndent()

        return databaseClient
            .sql(updateQuery)
            .bind("entity_id", entityId)
            .bind("attribute_name", attributeName)
            .bind("instance_id", instanceId)
            .bind("measured_value", measuredValue)
            .bind("payload", payload)
            .executeExpected {
                if (it == 0L)
                    ResourceNotFoundException(
                        attributeOrInstanceNotFoundMessage(attributeName, instanceId.toString())
                    ).left()
                else Unit.right()
            }
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
            WHERE temporal_entity_attribute = any( 
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
                if (it == 0L)
                    ResourceNotFoundException(
                        attributeOrInstanceNotFoundMessage(attributeName, instanceId.toString())
                    ).left()
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
                    ${if (datasetId != null) "AND dataset_id = :dataset_id" else "AND dataset_id IS NULL"}
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
