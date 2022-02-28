package com.egm.stellio.search.service

import com.egm.stellio.search.config.ApplicationProperties
import io.r2dbc.postgresql.codec.Json
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.net.URI

@Service
class EntityPayloadService(
    private val databaseClient: DatabaseClient,
    private val applicationProperties: ApplicationProperties
) {
    fun createEntityPayload(entityId: URI, entityPayload: String?): Mono<Int> =
        if (applicationProperties.entity.storePayloads)
            databaseClient.sql(
                """
                INSERT INTO entity_payload (entity_id, payload)
                VALUES (:entity_id, :payload)
                """.trimIndent()
            )
                .bind("entity_id", entityId)
                .bind("payload", entityPayload?.let { Json.of(entityPayload) })
                .fetch()
                .rowsUpdated()
        else
            Mono.just(1)

    fun upsertEntityPayload(entityId: URI, payload: String): Mono<Int> =
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
                .fetch()
                .rowsUpdated()
        else
            Mono.just(1)

    fun deleteEntityPayload(entityId: URI): Mono<Int> =
        databaseClient.sql(
            """
            DELETE FROM entity_payload WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .fetch()
            .rowsUpdated()
}
