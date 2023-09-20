package com.egm.stellio.search.scope

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.model.AttributeInstance.TemporalProperty
import com.egm.stellio.search.model.TemporalEntityAttribute.AttributeValueType
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.OperationNotSupportedException
import com.egm.stellio.shared.model.getScopes
import com.egm.stellio.shared.util.ExpandedAttributeInstances
import com.egm.stellio.shared.util.INCONSISTENT_VALUES_IN_AGGREGATION_MESSAGE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_PROPERTY
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.Sub
import io.r2dbc.postgresql.codec.Json
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.Duration
import java.time.ZonedDateTime

@Service
class ScopeService(
    private val databaseClient: DatabaseClient
) {

    @Transactional
    suspend fun createHistory(
        ngsiLdEntity: NgsiLdEntity,
        createdAt: ZonedDateTime,
        sub: Sub? = null
    ): Either<APIException, Unit> =
        if (!ngsiLdEntity.scopes.isNullOrEmpty())
            addHistoryEntry(
                ngsiLdEntity.id,
                ngsiLdEntity.scopes!!,
                TemporalProperty.CREATED_AT,
                createdAt,
                sub
            )
        else Unit.right()

    @Transactional
    suspend fun addHistoryEntry(
        entityId: URI,
        scopes: List<String>,
        temportalProperty: TemporalProperty,
        createdAt: ZonedDateTime,
        sub: Sub? = null
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            INSERT INTO scope_history (entity_id, value, time, time_property, sub)
            VALUES (:entity_id, array_to_json(:value), :time, :time_property, :sub)
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("value", scopes.toTypedArray())
            .bind("time", createdAt)
            .bind("time_property", temportalProperty.toString())
            .bind("sub", sub)
            .execute()

    @Transactional
    suspend fun retrieve(entityId: URI): Either<APIException, Pair<List<String>?, Json>> =
        databaseClient.sql(
            """
            SELECT scopes, payload
            FROM entity_payload
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .oneToResult {
                Pair(
                    toOptionalList(it["scopes"]),
                    toJson(it["payload"]),
                )
            }

    suspend fun retrieveHistory(
        entitiesIds: List<URI>,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        origin: ZonedDateTime? = null
    ): Either<APIException, List<ScopeInstanceResult>> {
        val temporalQuery = temporalEntitiesQuery.temporalQuery
        val sqlQueryBuilder = StringBuilder()

        sqlQueryBuilder.append(composeSearchSelectStatement(temporalEntitiesQuery, origin))

        sqlQueryBuilder.append(
            """
            FROM scope_history
            WHERE entity_id IN (:entities_ids)
            AND time_property = :time_property                    
            """.trimIndent()
        )

        when (temporalQuery.timerel) {
            TemporalQuery.Timerel.BEFORE -> sqlQueryBuilder.append(" AND time < '${temporalQuery.timeAt}'")
            TemporalQuery.Timerel.AFTER -> sqlQueryBuilder.append(" AND time > '${temporalQuery.timeAt}'")
            TemporalQuery.Timerel.BETWEEN -> sqlQueryBuilder.append(
                " AND time > '${temporalQuery.timeAt}' AND time < '${temporalQuery.endTimeAt}'"
            )
            null -> Unit
        }

        if (temporalEntitiesQuery.isAggregatedWithDefinedDuration())
            sqlQueryBuilder.append(" GROUP BY entity_id, origin")
        else if (temporalEntitiesQuery.withAggregatedValues)
            sqlQueryBuilder.append(" GROUP BY entity_id")
        else if (temporalQuery.lastN != null)
            // in order to get last instances, need to order by time desc
            // final ascending ordering of instances is done in query service
            sqlQueryBuilder.append(" ORDER BY time DESC LIMIT ${temporalQuery.lastN}")

        return databaseClient.sql(sqlQueryBuilder.toString())
            .bind("entities_ids", entitiesIds)
            .bind("time_property", temporalEntitiesQuery.temporalQuery.timeproperty.toString())
            .runCatching {
                this.allToMappedList { rowToScopeInstanceResult(it, temporalEntitiesQuery) }
            }.fold(
                { it.right() },
                { OperationNotSupportedException(INCONSISTENT_VALUES_IN_AGGREGATION_MESSAGE).left() }
            )
    }

    private fun composeSearchSelectStatement(
        temporalEntitiesQuery: TemporalEntitiesQuery,
        origin: ZonedDateTime?
    ): String = when {
        temporalEntitiesQuery.withAggregatedValues -> {
            val temporalQuery = temporalEntitiesQuery.temporalQuery
            val aggrPeriodDuration = temporalQuery.aggrPeriodDuration
            val allAggregates = temporalQuery.aggrMethods?.composeAggregationSelectClause(AttributeValueType.ARRAY)
            // if retrieving a temporal entity, origin is calculated beforehand as timeAt is optional in this case
            // if querying temporal entities, timeAt is mandatory and will be used if origin is null
            if (aggrPeriodDuration != WHOLE_TIME_RANGE_DURATION) {
                val computedOrigin = origin ?: temporalQuery.timeAt
                """
                SELECT entity_id,
                   time_bucket('$aggrPeriodDuration', time, TIMESTAMPTZ '${computedOrigin!!}') as origin,
                   $allAggregates
                """
            } else
                "SELECT entity_id, min(time) as origin, max(time) as endTime, $allAggregates "
        }
        temporalEntitiesQuery.temporalQuery.timeproperty == TemporalProperty.OBSERVED_AT -> {
            """
                SELECT entity_id, ARRAY(SELECT jsonb_array_elements_text(value)) as value, time
            """
        }
        else -> {
            """
                SELECT entity_id, ARRAY(SELECT jsonb_array_elements_text(value)) as value, time, sub
            """
        }
    }

    suspend fun selectOldestDate(entityId: URI, timeproperty: TemporalProperty): ZonedDateTime? =
        databaseClient
            .sql(
                """
                SELECT min(time)
                FROM scope_history
                WHERE entity_id = :entity_id
                AND time_property = :time_property
                """.trimIndent()
            )
            .bind("entity_id", entityId)
            .bind("time_property", timeproperty.name)
            .oneToResult { toZonedDateTime(it["first"]) }
            .getOrNull()

    private fun Json.replaceScopeValue(newScopeValue: Any): Map<String, Any> =
        this.deserializeExpandedPayload()
            .mapValues {
                if (it.key == NGSILD_SCOPE_PROPERTY) newScopeValue
                else it.value
            }

    private fun rowToScopeInstanceResult(
        row: Map<String, Any>,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): ScopeInstanceResult =
        if (temporalEntitiesQuery.withAggregatedValues) {
            val startDateTime = toZonedDateTime(row["origin"])
            val endDateTime =
                if (!temporalEntitiesQuery.isAggregatedWithDefinedDuration())
                    toZonedDateTime(row["endTime"])
                else
                    startDateTime.plus(Duration.parse(temporalEntitiesQuery.temporalQuery.aggrPeriodDuration!!))
            // in a row, there is the result for each requested aggregation method
            val values = temporalEntitiesQuery.temporalQuery.aggrMethods!!.map {
                val value = row["${it.method}_value"] ?: ""
                AggregatedScopeInstanceResult.AggregateResult(it, value, startDateTime, endDateTime)
            }
            AggregatedScopeInstanceResult(
                entityId = toUri(row["entity_id"]),
                values = values
            )
        } else if (temporalEntitiesQuery.withTemporalValues) {
            SimplifiedScopeInstanceResult(
                entityId = toUri(row["entity_id"]),
                scopes = toList(row["value"]),
                time = toZonedDateTime(row["time"])
            )
        } else {
            FullScopeInstanceResult(
                entityId = toUri(row["entity_id"]),
                scopes = toList(row["value"]),
                time = toZonedDateTime(row["time"]),
                timeproperty = temporalEntitiesQuery.temporalQuery.timeproperty.propertyName,
                sub = row["sub"] as? String
            )
        }

    @Transactional
    suspend fun update(
        entityId: URI,
        expandedAttributeInstances: ExpandedAttributeInstances,
        modifiedAt: ZonedDateTime,
        operationType: OperationType,
        sub: Sub? = null
    ): Either<APIException, UpdateResult> = either {
        val scopes = mapOf(NGSILD_SCOPE_PROPERTY to expandedAttributeInstances).getScopes()!!
        val (currentScopes, currentPayload) = retrieve(entityId).bind()

        val (updatedScopes, updatedPayload) = when (operationType) {
            OperationType.UPDATE_ATTRIBUTES -> {
                if (currentScopes != null) {
                    val updatedPayload = currentPayload.replaceScopeValue(expandedAttributeInstances)
                    Pair(scopes, updatedPayload)
                } else return@either UpdateResult(
                    updated = emptyList(),
                    notUpdated = listOf(
                        NotUpdatedDetails(
                            NGSILD_SCOPE_PROPERTY,
                            "Attribute does not exist and operation does not allow creating it"
                        )
                    )
                )
            }
            OperationType.APPEND_ATTRIBUTES, OperationType.MERGE_ENTITY -> {
                val newScopes = (currentScopes ?: emptyList()).toSet().plus(scopes).toList()
                val newPayload = newScopes.map { mapOf(JsonLdUtils.JSONLD_VALUE to it) }
                val updatedPayload = currentPayload.replaceScopeValue(newPayload)
                Pair(newScopes, updatedPayload)
            }
            OperationType.APPEND_ATTRIBUTES_OVERWRITE_ALLOWED,
            OperationType.MERGE_ENTITY_OVERWRITE_ALLOWED,
            OperationType.PARTIAL_ATTRIBUTE_UPDATE,
            OperationType.REPLACE_ATTRIBUTE -> {
                val updatedPayload = currentPayload.replaceScopeValue(expandedAttributeInstances)
                Pair(scopes, updatedPayload)
            }
            else -> Pair(null, Json.of("{}"))
        }

        updatedScopes?.let {
            val updateResult =
                performUpdate(entityId, updatedScopes, modifiedAt, serializeObject(updatedPayload)).bind()
            val temporalPropertyToAdd =
                if (currentScopes == null) TemporalProperty.CREATED_AT
                else TemporalProperty.MODIFIED_AT
            addHistoryEntry(entityId, it, temporalPropertyToAdd, modifiedAt, sub).bind()
            if (temporalPropertyToAdd == TemporalProperty.MODIFIED_AT)
                // as stated in 4.5.6: In case the Temporal Representation of the Scope is updated as the result of a
                // change from the Core API, the observedAt sub-Property should be set as a copy of the modifiedAt
                // sub-Property
                addHistoryEntry(entityId, it, TemporalProperty.OBSERVED_AT, modifiedAt, sub).bind()
            updateResult
        } ?: UpdateResult(
            emptyList(),
            listOf(NotUpdatedDetails(NGSILD_SCOPE_PROPERTY, "Unrecognized operation type: $operationType"))
        )
    }

    @Transactional
    internal suspend fun performUpdate(
        entityId: URI,
        scopes: List<String>,
        modifiedAt: ZonedDateTime,
        payload: String
    ): Either<APIException, UpdateResult> = either {
        databaseClient.sql(
            """
            UPDATE entity_payload
            SET scopes = :scopes,
                modified_at = :modified_at,
                payload = :payload
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("scopes", scopes.toTypedArray())
            .bind("modified_at", modifiedAt)
            .bind("payload", Json.of(payload))
            .execute()
            .map {
                updateResultFromDetailedResult(
                    listOf(
                        UpdateAttributeResult(
                            attributeName = NGSILD_SCOPE_PROPERTY,
                            updateOperationResult = UpdateOperationResult.APPENDED
                        )
                    )
                )
            }.bind()
    }

    @Transactional
    suspend fun replaceHistoryEntry(
        ngsiLdEntity: NgsiLdEntity,
        createdAt: ZonedDateTime,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        deleteHistory(ngsiLdEntity.id).bind()
        createHistory(ngsiLdEntity, createdAt, sub).bind()
    }

    @Transactional
    suspend fun delete(entityId: URI): Either<APIException, Unit> = either {
        databaseClient.sql(
            """
            UPDATE entity_payload
            SET scopes = null,
                payload = payload - '$NGSILD_SCOPE_PROPERTY'
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()
            .bind()

        deleteHistory(entityId).bind()
    }

    suspend fun deleteHistory(entityId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM scope_history
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()
}
