package com.egm.stellio.search.temporal.service

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import arrow.fx.coroutines.parMap
import com.egm.stellio.search.common.util.*
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.AttributeMetadata
import com.egm.stellio.search.entity.util.toAttributeMetadata
import com.egm.stellio.search.temporal.model.*
import com.egm.stellio.search.temporal.model.AggregatedAttributeInstanceResult.AggregateResult
import com.egm.stellio.search.temporal.model.TemporalQuery.Timerel
import com.egm.stellio.search.temporal.util.WHOLE_TIME_RANGE_DURATION
import com.egm.stellio.search.temporal.util.composeAggregationSelectClause
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.INCONSISTENT_VALUES_IN_AGGREGATION_MESSAGE
import com.egm.stellio.shared.util.attributeOrInstanceNotFoundMessage
import com.egm.stellio.shared.util.ngsiLdDateTime
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
                    (:time, :measured_value, :value, public.ST_GeomFromText(:geo_value), :attribute, 
                        :instance_id, :payload)
                ON CONFLICT (time, temporal_entity_attribute)
                DO UPDATE SET value = :value, measured_value = :measured_value, payload = :payload,
                              instance_id = :instance_id, geo_value = public.ST_GeomFromText(:geo_value)
                """.trimIndent()
            else if (attributeInstance.timeProperty == AttributeInstance.TemporalProperty.OBSERVED_AT)
                """
                INSERT INTO attribute_instance 
                    (time, measured_value, value, temporal_entity_attribute, 
                        instance_id, payload)
                VALUES 
                    (:time, :measured_value, :value, :attribute, 
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
                    (:time, :time_property, :measured_value, :value, public.ST_GeomFromText(:geo_value), 
                        :attribute, :instance_id, :payload, :sub)
                """.trimIndent()
            else
                """
                INSERT INTO attribute_instance_audit
                    (time, time_property, measured_value, value,
                        temporal_entity_attribute, instance_id, payload, sub)
                VALUES
                    (:time, :time_property, :measured_value, :value, 
                        :attribute, :instance_id, :payload, :sub)
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
            .bind("attribute", attributeInstance.attributeUuid)
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
        attributeUuid: UUID,
        attributeMetadata: AttributeMetadata,
        attributeValues: Map<String, List<Any>>
    ): Either<APIException, Unit> {
        val attributeInstance = AttributeInstance(
            attributeUuid = attributeUuid,
            time = attributeMetadata.observedAt!!,
            attributeMetadata = attributeMetadata,
            payload = attributeValues
        )
        return create(attributeInstance)
    }

    suspend fun search(
        temporalEntitiesQuery: TemporalEntitiesQuery,
        attribute: Attribute,
        origin: ZonedDateTime? = null
    ): Either<APIException, List<AttributeInstanceResult>> =
        search(temporalEntitiesQuery, listOf(attribute), origin)

    suspend fun search(
        temporalEntitiesQuery: TemporalEntitiesQuery,
        attributes: List<Attribute>,
        origin: ZonedDateTime? = null
    ): Either<APIException, List<AttributeInstanceResult>> {
        val temporalQuery = temporalEntitiesQuery.temporalQuery
        val sqlQueryBuilder = StringBuilder()

        sqlQueryBuilder.append(composeSearchSelectStatement(temporalQuery, attributes, origin))

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
            Timerel.BEFORE -> sqlQueryBuilder.append(" AND time < '${temporalQuery.timeAt}'")
            Timerel.AFTER -> sqlQueryBuilder.append(" AND time >= '${temporalQuery.timeAt}'")
            Timerel.BETWEEN -> sqlQueryBuilder.append(
                " AND time >= '${temporalQuery.timeAt}' AND time < '${temporalQuery.endTimeAt}'"
            )
            null -> Unit
        }

        if (temporalEntitiesQuery.isAggregatedWithDefinedDuration())
            sqlQueryBuilder.append(" GROUP BY temporal_entity_attribute, start")
        else if (temporalEntitiesQuery.withAggregatedValues)
            sqlQueryBuilder.append(" GROUP BY temporal_entity_attribute")

        if (temporalQuery.hasLastN())
            // in order to get first or last instances, need to order by time
            // final ascending ordering of instances is done in query service
            sqlQueryBuilder.append(" ORDER BY start DESC")
        else sqlQueryBuilder.append(" ORDER BY start ASC")

        sqlQueryBuilder.append(" LIMIT ${temporalQuery.instanceLimit}")

        val finalTemporalQuery = composeFinalTemporalQuery(attributes, sqlQueryBuilder.toString())

        return databaseClient.sql(finalTemporalQuery)
            .runCatching {
                this.allToMappedList { rowToAttributeInstanceResult(it, temporalEntitiesQuery) }
            }.fold(
                { it.right() },
                {
                    OperationNotSupportedException(
                        it.cause?.message ?: INCONSISTENT_VALUES_IN_AGGREGATION_MESSAGE
                    ).left()
                }
            )
    }

    private fun composeFinalTemporalQuery(
        attributes: List<Attribute>,
        aiLateralQuery: String
    ): String {
        val attributesIds = attributes.joinToString(",") { "('${it.id}'::uuid)" }

        return """
        SELECT ai_limited.*
        FROM (
            SELECT distinct(id)
            FROM (VALUES $attributesIds) as t (id)
        ) teas
        JOIN LATERAL (
            $aiLateralQuery
        ) ai_limited ON true;
        """.trimIndent()
    }

    private fun composeSearchSelectStatement(
        temporalQuery: TemporalQuery,
        attributes: List<Attribute>,
        origin: ZonedDateTime?
    ) = when {
        temporalQuery.aggrPeriodDuration != null -> {
            val aggrPeriodDuration = temporalQuery.aggrPeriodDuration
            val allAggregates = temporalQuery.aggrMethods
                ?.composeAggregationSelectClause(attributes[0].attributeValueType)
            // if retrieving a temporal entity, origin is calculated beforehand as timeAt is optional in this case
            // if querying temporal entities, timeAt is mandatory and will be used if origin is null
            if (aggrPeriodDuration != WHOLE_TIME_RANGE_DURATION) {
                val computedOrigin = origin ?: temporalQuery.timeAt
                """
                SELECT temporal_entity_attribute,
                    public.time_bucket('$aggrPeriodDuration', time, TIMESTAMPTZ '${computedOrigin!!}') as start,
                    $allAggregates
                """.trimIndent()
            } else
                "SELECT temporal_entity_attribute, min(time) as start, max(time) as end, $allAggregates "
        }
        else -> {
            val valueColumn = when (attributes[0].attributeValueType) {
                Attribute.AttributeValueType.NUMBER -> "measured_value as value"
                Attribute.AttributeValueType.GEOMETRY -> "public.ST_AsText(geo_value) as value"
                else -> "value"
            }
            val subColumn = when (temporalQuery.timeproperty) {
                AttributeInstance.TemporalProperty.OBSERVED_AT -> null
                else -> "sub"
            }
            "SELECT " + listOfNotNull("temporal_entity_attribute", "time as start", valueColumn, subColumn)
                .joinToString(",")
        }
    }

    suspend fun selectOldestDate(
        temporalQuery: TemporalQuery,
        attributes: List<Attribute>
    ): ZonedDateTime? {
        val attributesIds = attributes.joinToString(",") { "'${it.id}'" }
        var selectQuery =
            """
            SELECT min(time) as first
            """.trimIndent()

        selectQuery =
            if (temporalQuery.timeproperty == AttributeInstance.TemporalProperty.OBSERVED_AT)
                selectQuery.plus(
                    """
                    FROM attribute_instance
                    WHERE temporal_entity_attribute IN($attributesIds)
                    """
                )
            else
                selectQuery.plus(
                    """
                    FROM attribute_instance_audit
                    WHERE temporal_entity_attribute IN($attributesIds)
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
    ): AttributeInstanceResult =
        if (temporalEntitiesQuery.withAggregatedValues) {
            val startDateTime = toZonedDateTime(row["start"])
            val endDateTime =
                if (!temporalEntitiesQuery.isAggregatedWithDefinedDuration())
                    toZonedDateTime(row["end"])
                else
                    startDateTime.plus(temporalEntitiesQuery.computeAggrPeriodDuration())
            // in a row, there is the result for each requested aggregation method
            val values = temporalEntitiesQuery.temporalQuery.aggrMethods!!.map {
                val value = row["${it.method}_value"] ?: ""
                AggregateResult(it, value, startDateTime, endDateTime)
            }
            AggregatedAttributeInstanceResult(
                attributeUuid = toUuid(row["temporal_entity_attribute"]),
                values = values
            )
        } else if (temporalEntitiesQuery.withTemporalValues)
            SimplifiedAttributeInstanceResult(
                attributeUuid = toUuid(row["temporal_entity_attribute"]),
                // the type of the value of a property may have changed in the history (e.g., from number to string)
                // in this case, just display an empty value (something happened, but we can't display it)
                value = row["value"] ?: "",
                time = toZonedDateTime(row["start"])
            )
        else FullAttributeInstanceResult(
            attributeUuid = toUuid(row["temporal_entity_attribute"]),
            payload = toJsonString(row["payload"]),
            time = toZonedDateTime(row["start"]),
            timeproperty = temporalEntitiesQuery.temporalQuery.timeproperty.propertyName,
            sub = row["sub"] as? String
        )

    @Transactional
    suspend fun modifyAttributeInstance(
        entityId: URI,
        attributeName: ExpandedTerm,
        instanceId: URI,
        expandedAttributeInstances: ExpandedAttributeInstances
    ): Either<APIException, Unit> = either {
        val attributeUUID = retrieveAttributeUUID(entityId, attributeName, instanceId).bind()
        val ngsiLdAttribute = expandedAttributeInstances.toNgsiLdAttribute(attributeName).bind()
        val ngsiLdAttributeInstance = ngsiLdAttribute.getAttributeInstances()[0]
        val attributeMetadata = ngsiLdAttributeInstance.toAttributeMetadata().bind()

        deleteInstance(entityId, attributeName, instanceId).bind()
        create(
            AttributeInstance(
                attributeUuid = attributeUUID,
                time = attributeMetadata.observedAt!!,
                attributeMetadata = attributeMetadata,
                modifiedAt = ngsiLdDateTime(),
                instanceId = instanceId,
                payload = expandedAttributeInstances.first()
            )
        ).bind()
    }

    private suspend fun retrieveAttributeUUID(
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
    ): Either<APIException, Unit> = either {
        attributesInstancesTables.parMap { attributeInstanceTable ->
            val deleteQuery =
                """
                DELETE FROM $attributeInstanceTable
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
                .bind()
        }.map { }
    }

    @Transactional
    suspend fun deleteAllInstancesOfAttribute(
        entityId: URI,
        attributeName: ExpandedTerm
    ): Either<APIException, Unit> = either {
        attributesInstancesTables.parMap { attributeInstanceTable ->
            val deleteQuery =
                """
                DELETE FROM $attributeInstanceTable
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
                .bind()
        }.map { }
    }

    @Transactional
    suspend fun deleteInstancesOfEntity(
        uuids: List<UUID>
    ): Either<APIException, Unit> = either {
        attributesInstancesTables.parMap { attributeInstanceTable ->
            val deleteQuery =
                """
                DELETE FROM $attributeInstanceTable
                WHERE temporal_entity_attribute IN (:uuids)
                """.trimIndent()

            databaseClient
                .sql(deleteQuery)
                .bind("uuids", uuids)
                .execute()
                .bind()
        }.map { }
    }
}
