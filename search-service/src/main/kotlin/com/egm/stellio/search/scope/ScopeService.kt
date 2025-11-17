package com.egm.stellio.search.scope

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.common.util.allToMappedList
import com.egm.stellio.search.common.util.deserializeExpandedPayload
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toJson
import com.egm.stellio.search.common.util.toList
import com.egm.stellio.search.common.util.toOptionalList
import com.egm.stellio.search.common.util.toOptionalZonedDateTime
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.common.util.toZonedDateTime
import com.egm.stellio.search.entity.model.Attribute.AttributeValueType
import com.egm.stellio.search.entity.model.AttributeOperationResult
import com.egm.stellio.search.entity.model.FailedAttributeOperationResult
import com.egm.stellio.search.entity.model.OperationStatus
import com.egm.stellio.search.entity.model.OperationType
import com.egm.stellio.search.entity.model.SucceededAttributeOperationResult
import com.egm.stellio.search.temporal.model.AttributeInstance.TemporalProperty
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.search.temporal.util.WHOLE_TIME_RANGE_DURATION
import com.egm.stellio.search.temporal.util.composeAggregationSelectClause
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedAttributeInstances
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_SCOPE_IRI
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.OperationNotSupportedException
import com.egm.stellio.shared.model.Scope
import com.egm.stellio.shared.model.getScopes
import com.egm.stellio.shared.util.INCONSISTENT_VALUES_IN_AGGREGATION_MESSAGE
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.ngsiLdDateTime
import io.r2dbc.postgresql.codec.Json
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.Duration
import java.time.ZonedDateTime

@Service
class ScopeService(
    private val databaseClient: DatabaseClient,
    private val searchProperties: SearchProperties,
    private val authorizationService: AuthorizationService
) {

    @Transactional
    suspend fun createHistory(
        ngsiLdEntity: NgsiLdEntity,
        createdAt: ZonedDateTime
    ): Either<APIException, Unit> = either {
        if (!ngsiLdEntity.scopes.isNullOrEmpty()) {
            addHistoryEntry(
                ngsiLdEntity.id,
                ngsiLdEntity.scopes!!,
                TemporalProperty.CREATED_AT,
                createdAt
            ).bind()
            authorizationService.createScopesOwnerRights(ngsiLdEntity.scopes!!).bind()
        }
    }

    @Transactional
    suspend fun addHistoryEntry(
        entityId: URI,
        scopes: List<Scope>,
        temporalProperty: TemporalProperty,
        createdAt: ZonedDateTime
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
            .bind("time_property", temporalProperty.toString())
            .bind("sub", getSubFromSecurityContext())
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
            TemporalQuery.Timerel.AFTER -> sqlQueryBuilder.append(" AND time >= '${temporalQuery.timeAt}'")
            TemporalQuery.Timerel.BETWEEN -> sqlQueryBuilder.append(
                " AND time >= '${temporalQuery.timeAt}' AND time < '${temporalQuery.endTimeAt}'"
            )
            null -> Unit
        }

        if (temporalEntitiesQuery.isAggregatedWithDefinedDuration())
            sqlQueryBuilder.append(" GROUP BY entity_id, start")
        else if (temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.AGGREGATED_VALUES)
            sqlQueryBuilder.append(" GROUP BY entity_id")
        if (temporalQuery.hasLastN())
            // in order to get first or last instances, need to order by time
            // final ascending ordering of instances is done in query service
            sqlQueryBuilder.append(" ORDER BY start DESC")
        else sqlQueryBuilder.append(" ORDER BY start ASC")

        sqlQueryBuilder.append(" LIMIT ${temporalQuery.instanceLimit}")

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
        temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.AGGREGATED_VALUES -> {
            val temporalQuery = temporalEntitiesQuery.temporalQuery
            val aggrPeriodDuration = temporalQuery.aggrPeriodDuration
            val allAggregates = temporalQuery.aggrMethods?.composeAggregationSelectClause(AttributeValueType.ARRAY)
            // if retrieving a temporal entity, origin is calculated beforehand as timeAt is optional in this case
            // if querying temporal entities, timeAt is mandatory and will be used if origin is null
            if (aggrPeriodDuration != WHOLE_TIME_RANGE_DURATION) {
                val computedOrigin = origin ?: temporalQuery.timeAt
                """
                SELECT entity_id,
                   public.time_bucket('$aggrPeriodDuration', time, '${searchProperties.timezoneForTimeBuckets}', TIMESTAMPTZ '${computedOrigin!!}') as start,
                   $allAggregates
                """
            } else
                "SELECT entity_id, min(time) as start, max(time) as end, $allAggregates "
        }
        temporalEntitiesQuery.temporalQuery.timeproperty == TemporalProperty.OBSERVED_AT -> {
            """
                SELECT entity_id, ARRAY(SELECT jsonb_array_elements_text(value)) as value, time as start
            """
        }
        else -> {
            """
                SELECT entity_id, ARRAY(SELECT jsonb_array_elements_text(value)) as value, time as start, sub
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
            .oneToResult { toOptionalZonedDateTime(it["first"]) }
            .getOrNull()

    private fun Json.replaceScopeValue(newScopeValue: Any): Map<String, Any> =
        this.deserializeExpandedPayload()
            .mapValues {
                if (it.key == NGSILD_SCOPE_IRI) newScopeValue
                else it.value
            }

    private fun rowToScopeInstanceResult(
        row: Map<String, Any>,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): ScopeInstanceResult =
        when (temporalEntitiesQuery.temporalRepresentation) {
            TemporalRepresentation.AGGREGATED_VALUES -> {
                val startDateTime = toZonedDateTime(row["start"])
                val endDateTime =
                    if (!temporalEntitiesQuery.isAggregatedWithDefinedDuration())
                        toZonedDateTime(row["end"])
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
            }
            TemporalRepresentation.TEMPORAL_VALUES -> {
                SimplifiedScopeInstanceResult(
                    entityId = toUri(row["entity_id"]),
                    scopes = toList(row["value"]),
                    time = toZonedDateTime(row["start"])
                )
            }
            else -> {
                FullScopeInstanceResult(
                    entityId = toUri(row["entity_id"]),
                    scopes = toList(row["value"]),
                    time = toZonedDateTime(row["start"]),
                    timeproperty = temporalEntitiesQuery.temporalQuery.timeproperty.propertyName,
                    sub = row["sub"] as? String
                )
            }
        }

    @Transactional
    suspend fun update(
        entityId: URI,
        expandedAttributeInstances: ExpandedAttributeInstances,
        modifiedAt: ZonedDateTime,
        operationType: OperationType
    ): Either<APIException, AttributeOperationResult> = either {
        val scopes = mapOf(NGSILD_SCOPE_IRI to expandedAttributeInstances).getScopes()!!
        val (currentScopes, currentPayload) = retrieve(entityId).bind()

        val (updatedScopes, updatedPayload) = when (operationType) {
            OperationType.UPDATE_ATTRIBUTES -> {
                if (currentScopes != null) {
                    val updatedPayload = currentPayload.replaceScopeValue(expandedAttributeInstances)
                    Pair(scopes, updatedPayload)
                } else return@either FailedAttributeOperationResult(
                    attributeName = NGSILD_SCOPE_IRI,
                    operationStatus = OperationStatus.FAILED,
                    errorMessage = "Scope does not exist and operation does not allow creating it"
                )
            }
            OperationType.APPEND_ATTRIBUTES, OperationType.MERGE_ENTITY -> {
                val newScopes = (currentScopes ?: emptyList()).toSet().plus(scopes).toList()
                val newPayload = newScopes.map { mapOf(JSONLD_VALUE_KW to it) }
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
            val operationResult =
                performUpdate(entityId, updatedScopes, modifiedAt, serializeObject(updatedPayload)).bind()
            val temporalPropertyToAdd =
                if (currentScopes == null) TemporalProperty.CREATED_AT
                else TemporalProperty.MODIFIED_AT
            addHistoryEntry(entityId, it, temporalPropertyToAdd, modifiedAt).bind()
            if (temporalPropertyToAdd == TemporalProperty.MODIFIED_AT)
                // as stated in 4.5.6: In case the Temporal Representation of the Scope is updated as the result of a
                // change from the Core API, the observedAt sub-Property should be set as a copy of the modifiedAt
                // sub-Property
                addHistoryEntry(entityId, it, TemporalProperty.OBSERVED_AT, modifiedAt).bind()
            authorizationService.createScopesOwnerRights(it).bind()
            operationResult
        } ?: FailedAttributeOperationResult(
            attributeName = NGSILD_SCOPE_IRI,
            operationStatus = OperationStatus.FAILED,
            errorMessage = "Unrecognized operation type on scope: $operationType"
        )
    }

    @Transactional
    internal suspend fun performUpdate(
        entityId: URI,
        scopes: List<Scope>,
        modifiedAt: ZonedDateTime,
        payload: String
    ): Either<APIException, SucceededAttributeOperationResult> = either {
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
                SucceededAttributeOperationResult(
                    attributeName = NGSILD_SCOPE_IRI,
                    operationStatus = OperationStatus.CREATED,
                    newExpandedValue = mapOf(NGSILD_SCOPE_IRI to scopes.toList())
                )
            }.bind()
    }

    @Transactional
    suspend fun replace(
        ngsiLdEntity: NgsiLdEntity,
        createdAt: ZonedDateTime
    ): Either<APIException, Unit> = either {
        createHistory(ngsiLdEntity, createdAt).bind()
    }

    @Transactional
    suspend fun delete(entityId: URI): Either<APIException, List<AttributeOperationResult>> = either {
        databaseClient.sql(
            """
            UPDATE entity_payload
            SET scopes = null,
                payload = payload - '$NGSILD_SCOPE_IRI'
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()
            .bind()

        addHistoryEntry(
            entityId,
            emptyList(),
            TemporalProperty.DELETED_AT,
            ngsiLdDateTime()
        ).bind()

        listOf(
            SucceededAttributeOperationResult(
                NGSILD_SCOPE_IRI,
                null,
                OperationStatus.DELETED,
                mapOf(NGSILD_SCOPE_IRI to listOf())
            )
        )
    }

    @Transactional
    suspend fun permanentlyDelete(entityId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM scope_history
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()
}
