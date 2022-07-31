package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.right
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.util.execute
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import io.r2dbc.postgresql.codec.Json
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import java.net.URI

@Service
class EntityPayloadService(
    private val databaseClient: DatabaseClient,
    private val applicationProperties: ApplicationProperties
) {
    suspend fun createEntityPayload(entityId: URI, deserializedPayload: Map<String, Any>): Either<APIException, Unit> =
        createEntityPayload(entityId, serializeObject(deserializedPayload))

    suspend fun createEntityPayload(entityId: URI, entityPayload: String?): Either<APIException, Unit> =
        if (applicationProperties.entity.storePayloads)
            databaseClient.sql(
                """
                INSERT INTO entity_payload (entity_id, payload)
                VALUES (:entity_id, :payload)
                """.trimIndent()
            )
                .bind("entity_id", entityId)
                .bind("payload", entityPayload?.let { Json.of(entityPayload) })
                .execute()
        else Unit.right()

    suspend fun upsertEntityPayload(entityId: URI, payload: String): Either<APIException, Unit> =
        if (applicationProperties.entity.storePayloads)
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
        else Unit.right()

    suspend fun deleteEntityPayload(entityId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM entity_payload WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()
}
