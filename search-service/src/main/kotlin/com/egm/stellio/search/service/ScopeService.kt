package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.getScopes
import com.egm.stellio.shared.util.ExpandedAttributeInstances
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.Sub
import io.r2dbc.postgresql.codec.Json
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
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
                AttributeInstance.TemporalProperty.CREATED_AT,
                createdAt,
                sub
            )
        else Unit.right()

    @Transactional
    suspend fun addHistoryEntry(
        entityId: URI,
        scopes: List<String>,
        temportalProperty: AttributeInstance.TemporalProperty,
        createdAt: ZonedDateTime,
        sub: Sub? = null
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            INSERT INTO scope_history (entity_id, scopes, time, time_property, sub)
            VALUES (:entity_id, :scopes, :time, :time_property, :sub)
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("scopes", scopes.toTypedArray())
            .bind("time", createdAt)
            .bind("time_property", temportalProperty.toString())
            .bind("sub", sub)
            .execute()

    @Transactional
    suspend fun retrieveScopes(
        entityId: URI
    ): Either<APIException, Pair<List<String>?, Json>> =
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

    @Transactional
    suspend fun update(
        entityId: URI,
        expandedAttributeInstances: ExpandedAttributeInstances,
        modifiedAt: ZonedDateTime,
        operationType: OperationType,
        sub: Sub? = null
    ): Either<APIException, UpdateResult> = either {
        val scopes = mapOf(JsonLdUtils.NGSILD_SCOPE_PROPERTY to expandedAttributeInstances).getScopes()!!
        val (currentScopes, currentPayload) = retrieveScopes(entityId).bind()

        val (updatedScopes, updatedPayload) = when (operationType) {
            OperationType.UPDATE_ATTRIBUTES -> {
                if (currentScopes != null) {
                    val updatedPayload = currentPayload.deserializeExpandedPayload()
                        .mapValues {
                            if (it.key == JsonLdUtils.NGSILD_SCOPE_PROPERTY)
                                expandedAttributeInstances
                            else it
                        }
                    Pair(scopes, updatedPayload)
                } else return@either UpdateResult(
                    updated = emptyList(),
                    notUpdated = listOf(
                        NotUpdatedDetails(
                            JsonLdUtils.NGSILD_SCOPE_PROPERTY,
                            "Attribute does not exist and operation does not allow creating it"
                        )
                    )
                )
            }
            OperationType.APPEND_ATTRIBUTES, OperationType.MERGE_ENTITY -> {
                val newScopes = (currentScopes ?: emptyList()).toSet().plus(scopes).toList()
                val newPayload = newScopes.map { mapOf(JsonLdUtils.JSONLD_VALUE to it) }
                val updatedPayload = currentPayload.deserializeExpandedPayload()
                    .mapValues {
                        if (it.key == JsonLdUtils.NGSILD_SCOPE_PROPERTY)
                            newPayload
                        else it
                    }
                Pair(newScopes, updatedPayload)
            }
            OperationType.APPEND_ATTRIBUTES_OVERWRITE_ALLOWED,
            OperationType.MERGE_ENTITY_OVERWRITE_ALLOWED,
            OperationType.PARTIAL_ATTRIBUTE_UPDATE,
            OperationType.REPLACE_ATTRIBUTE -> {
                val updatedPayload = currentPayload.deserializeExpandedPayload()
                    .mapValues {
                        if (it.key == JsonLdUtils.NGSILD_SCOPE_PROPERTY)
                            expandedAttributeInstances
                        else it
                    }
                Pair(scopes, updatedPayload)
            }
            else -> Pair(null, Json.of("{}"))
        }

        updatedScopes?.let {
            val updateResult =
                performUpdate(entityId, updatedScopes, modifiedAt, serializeObject(updatedPayload)).bind()
            addHistoryEntry(entityId, it, AttributeInstance.TemporalProperty.MODIFIED_AT, modifiedAt, sub).bind()
            // as stated in 4.5.6: In case the Temporal Representation of the Scope is updated as the result of a
            // change from the Core API, the observedAt sub-Property should be set as a copy of the modifiedAt
            // sub-Property
            addHistoryEntry(entityId, it, AttributeInstance.TemporalProperty.OBSERVED_AT, modifiedAt, sub).bind()
            updateResult
        } ?: UpdateResult(
            emptyList(),
            listOf(NotUpdatedDetails(JsonLdUtils.NGSILD_SCOPE_PROPERTY, "Unrecognized operation type: $operationType"))
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
                            attributeName = JsonLdUtils.NGSILD_SCOPE_PROPERTY,
                            updateOperationResult = UpdateOperationResult.APPENDED
                        )
                    )
                )
            }.bind()
    }

    @Transactional
    suspend fun replaceScopeHistoryEntry(
        ngsiLdEntity: NgsiLdEntity,
        createdAt: ZonedDateTime,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        deleteScopeHistory(ngsiLdEntity.id).bind()
        createHistory(ngsiLdEntity, createdAt, sub).bind()
    }

    @Transactional
    suspend fun deleteScopes(entityId: URI): Either<APIException, Unit> = either {
        databaseClient.sql(
            """
            UPDATE entity_payload
            SET scopes = null
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()
            .bind()

        deleteScopeHistory(entityId).bind()
    }

    suspend fun deleteScopeHistory(entityId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM scope_history
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()
}
