package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.entityNotFoundMessage
import io.r2dbc.postgresql.codec.Json
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.ZonedDateTime

@Service
class EntityPayloadService(
    private val databaseClient: DatabaseClient
) {
    @Transactional
    suspend fun createEntityPayload(
        entityId: URI,
        types: List<ExpandedTerm>,
        createdAt: ZonedDateTime,
        jsonLdEntity: JsonLdEntity
    ): Either<APIException, Unit> =
        createEntityPayload(entityId, types, createdAt, serializeObject(jsonLdEntity), jsonLdEntity.contexts)

    suspend fun createEntityPayload(
        entityId: URI,
        types: List<ExpandedTerm>,
        createdAt: ZonedDateTime,
        entityPayload: String?,
        contexts: List<String>
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            INSERT INTO entity_payload (entity_id, types, created_at, payload, contexts)
            VALUES (:entity_id, :types, :created_at, :payload, :contexts)
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("types", types.toTypedArray())
            .bind("created_at", createdAt)
            .bind("payload", entityPayload?.let { Json.of(entityPayload) })
            .bind("contexts", contexts.toTypedArray())
            .execute()

    suspend fun retrieve(entityId: URI): Either<APIException, EntityPayload> =
        databaseClient.sql(
            """
            SELECT * from entity_payload
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .oneToResult { rowToEntityPaylaod(it) }

    private fun rowToEntityPaylaod(row: Map<String, Any>): EntityPayload =
        EntityPayload(
            entityId = toUri(row["entity_id"]),
            types = toList(row["types"]),
            createdAt = toZonedDateTime(row["created_at"]),
            modifiedAt = toOptionalZonedDateTime(row["modified_at"]),
            contexts = toList(row["contexts"])
        )

    suspend fun getTypes(entityId: URI): Either<APIException, List<ExpandedTerm>> {
        val selectQuery =
            """
            SELECT types
            FROM entity_payload
            WHERE entity_id = :entity_id
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", entityId)
            .oneToResult(ResourceNotFoundException(entityNotFoundMessage(entityId.toString()))) {
                (it["types"] as Array<ExpandedTerm>).toList()
            }
    }

    @Transactional
    suspend fun updateTypes(
        entityId: URI,
        types: List<ExpandedTerm>,
        allowEmptyListOfTypes: Boolean = true
    ): Either<APIException, UpdateResult> =
        either {
            val currentTypes = getTypes(entityId).bind()
            // when dealing with an entity update, list of types can be empty if no change of type is requested
            if (currentTypes.sorted() == types.sorted() || types.isEmpty() && allowEmptyListOfTypes)
                UpdateResult(emptyList(), emptyList()).right().bind()
            if (!types.containsAll(currentTypes)) {
                val removedTypes = currentTypes.minus(types)
                updateResultFromDetailedResult(
                    listOf(
                        UpdateAttributeResult(
                            attributeName = JsonLdUtils.JSONLD_TYPE,
                            updateOperationResult = UpdateOperationResult.FAILED,
                            errorMessage = "A type cannot be removed from an entity: $removedTypes have been removed"
                        )
                    )
                ).right().bind()
            }

            databaseClient.sql(
                """
                UPDATE entity_payload
                SET types = :types
                WHERE entity_id = :entity_id
                """.trimIndent()
            )
                .bind("entity_id", entityId)
                .bind("types", types.toTypedArray())
                .execute()
                .map {
                    updateResultFromDetailedResult(
                        listOf(
                            UpdateAttributeResult(
                                attributeName = JsonLdUtils.JSONLD_TYPE,
                                updateOperationResult = UpdateOperationResult.APPENDED
                            )
                        )
                    )
                }.bind()
        }

    @Transactional
    suspend fun updateLastModificationDate(entityUri: URI, modifiedAt: ZonedDateTime): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE entity_payload
            SET modified_at = :modified_at
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityUri)
            .bind("modified_at", modifiedAt)
            .execute()

    @Transactional
    suspend fun upsertEntityPayload(entityId: URI, payload: String): Either<APIException, Unit> =
        databaseClient.sql(
            """
            INSERT INTO entity_payload (entity_id, payload)
            VALUES (:entity_id, :payload)
            ON CONFLICT (entity_id)
            DO UPDATE SET payload = :payload
            """.trimIndent()
        )
            .bind("payload", Json.of(payload))
            .bind("entity_id", entityId)
            .execute()

    @Transactional
    suspend fun deleteEntityPayload(entityId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM entity_payload WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()
}
