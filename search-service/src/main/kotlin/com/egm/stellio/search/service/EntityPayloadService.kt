package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonUtils.deserializeExpandedPayload
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import io.r2dbc.postgresql.codec.Json
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.ZoneOffset
import java.time.ZonedDateTime

@Service
class EntityPayloadService(
    private val databaseClient: DatabaseClient
) {
    @Transactional
    suspend fun createEntityPayload(
        ngsiLdEntity: NgsiLdEntity,
        createdAt: ZonedDateTime,
        jsonLdEntity: JsonLdEntity
    ): Either<APIException, Unit> = either {
        val specificAccessPolicy = ngsiLdEntity.properties.find { it.name == AuthContextModel.AUTH_PROP_SAP }
            ?.let { getSpecificAccessPolicy(it) }
            ?.bind()
        createEntityPayload(
            ngsiLdEntity.id,
            ngsiLdEntity.types,
            createdAt,
            serializeObject(jsonLdEntity.properties.addDateTimeProperty(NGSILD_CREATED_AT_PROPERTY, createdAt)),
            jsonLdEntity.contexts,
            specificAccessPolicy
        ).bind()
    }

    suspend fun createEntityPayload(
        entityId: URI,
        types: List<ExpandedTerm>,
        createdAt: ZonedDateTime,
        entityPayload: String,
        contexts: List<String>,
        specificAccessPolicy: SpecificAccessPolicy? = null
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            INSERT INTO entity_payload (entity_id, types, created_at, payload, contexts, specific_access_policy)
            VALUES (:entity_id, :types, :created_at, :payload, :contexts, :specific_access_policy)
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("types", types.toTypedArray())
            .bind("created_at", createdAt)
            .bind("payload", Json.of(entityPayload))
            .bind("contexts", contexts.toTypedArray())
            .bind("specific_access_policy", specificAccessPolicy?.toString())
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

    suspend fun retrieve(entitiesIds: List<URI>): List<EntityPayload> =
        databaseClient.sql(
            """
            SELECT * from entity_payload
            WHERE entity_id IN (:entities_ids)
            """.trimIndent()
        )
            .bind("entities_ids", entitiesIds)
            .allToMappedList { rowToEntityPaylaod(it) }

    private fun rowToEntityPaylaod(row: Map<String, Any>): EntityPayload =
        EntityPayload(
            entityId = toUri(row["entity_id"]),
            types = toList(row["types"]),
            createdAt = toZonedDateTime(row["created_at"]),
            modifiedAt = toOptionalZonedDateTime(row["modified_at"]),
            contexts = toList(row["contexts"]),
            entityPayload = toJsonString(row["payload"]),
            specificAccessPolicy = toOptionalEnum<SpecificAccessPolicy>(row["specific_access_policy"])
        )

    suspend fun checkEntityExistence(
        entityId: URI,
        inverse: Boolean = false
    ): Either<APIException, Unit> {
        val selectQuery =
            """
                select 
                    exists(
                        select 1 
                        from entity_payload 
                        where entity_id = :entity_id
                    ) as entityExists;
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", entityId)
            .oneToResult { it["entityExists"] as Boolean }
            .flatMap {
                if (it && !inverse || !it && inverse)
                    Unit.right()
                else if (it)
                    AlreadyExistsException(entityAlreadyExistsMessage(entityId.toString())).left()
                else
                    ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()
            }
    }

    suspend fun hasSpecificAccessPolicies(
        entityId: URI,
        specificAccessPolicies: List<SpecificAccessPolicy>
    ): Either<APIException, Boolean> =
        databaseClient.sql(
            """
            SELECT count(entity_id) as count
            FROM entity_payload
            WHERE entity_id = :entity_id
            AND specific_access_policy IN (:specific_access_policies)
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("specific_access_policies", specificAccessPolicies.map { it.toString() })
            .oneToResult { it["count"] as Long > 0 }

    suspend fun filterExistingEntitiesAsIds(entitiesIds: List<URI>): List<URI> {
        if (entitiesIds.isEmpty()) {
            return emptyList()
        }

        val query = """
            select entity_id 
            from entity_payload
            where entity_id in (:entities_ids)
        """.trimIndent()

        return databaseClient
            .sql(query)
            .bind("entities_ids", entitiesIds)
            .allToMappedList { toUri(it["entity_id"]) }
    }

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
        newTypes: List<ExpandedTerm>,
        allowEmptyListOfTypes: Boolean = true
    ): Either<APIException, UpdateResult> =
        either {
            val entityPayload = retrieve(entityId).bind()
            val currentTypes = entityPayload.types
            // when dealing with an entity update, list of types can be empty if no change of type is requested
            if (currentTypes.sorted() == newTypes.sorted() || newTypes.isEmpty() && allowEmptyListOfTypes)
                return@either UpdateResult(emptyList(), emptyList())
            if (!newTypes.containsAll(currentTypes)) {
                val removedTypes = currentTypes.minus(newTypes)
                return@either updateResultFromDetailedResult(
                    listOf(
                        UpdateAttributeResult(
                            attributeName = JsonLdUtils.JSONLD_TYPE,
                            updateOperationResult = UpdateOperationResult.FAILED,
                            errorMessage = "A type cannot be removed from an entity: $removedTypes have been removed"
                        )
                    )
                )
            }

            val updatedPayload = entityPayload.entityPayload.deserializeExpandedPayload()
                .mapValues {
                    if (it.key == JSONLD_TYPE)
                        newTypes
                    else it
                }

            databaseClient.sql(
                """
                UPDATE entity_payload
                SET types = :types,
                    modified_at = :modified_at,
                    payload = :payload
                WHERE entity_id = :entity_id
                """.trimIndent()
            )
                .bind("entity_id", entityId)
                .bind("types", newTypes.toTypedArray())
                .bind("modified_at", ZonedDateTime.now(ZoneOffset.UTC))
                .bind("payload", Json.of(serializeObject(updatedPayload)))
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
    suspend fun updateState(
        entityUri: URI,
        modifiedAt: ZonedDateTime,
        temporalEntityAttributes: List<TemporalEntityAttribute>
    ): Either<APIException, Unit> =
        retrieve(entityUri)
            .map { entityPayload ->
                val payload = buildJsonLdEntity(temporalEntityAttributes, entityPayload)
                databaseClient.sql(
                    """
                    UPDATE entity_payload
                    SET modified_at = :modified_at,
                        payload = :payload
                    WHERE entity_id = :entity_id
                    """.trimIndent()
                )
                    .bind("entity_id", entityUri)
                    .bind("modified_at", modifiedAt)
                    .bind("payload", Json.of(serializeObject(payload)))
                    .execute()
            }

    private fun buildJsonLdEntity(
        temporalEntityAttributes: List<TemporalEntityAttribute>,
        entityPayload: EntityPayload
    ): Map<String, Any> {
        val entityCoreAttributes = entityPayload.serializeProperties(withSysAttrs = true)
        val expandedAttributes = temporalEntityAttributes
            .groupBy {
                it.attributeName
            }
            .mapValues { teas ->
                teas.value.map { tea ->
                    tea.payload.deserializeExpandedPayload()
                        .addSysAttrs(withSysAttrs = true, tea.createdAt, tea.modifiedAt)
                }
            }

        return entityCoreAttributes.plus(expandedAttributes)
    }

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

    suspend fun updateSpecificAccessPolicy(
        entityId: URI,
        ngsiLdAttribute: NgsiLdAttribute
    ): Either<APIException, Unit> = either {
        val specificAccessPolicy = getSpecificAccessPolicy(ngsiLdAttribute).bind()
        databaseClient.sql(
            """
            UPDATE entity_payload
            SET specific_access_policy = :specific_access_policy
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("specific_access_policy", specificAccessPolicy.toString())
            .execute()
    }

    suspend fun removeSpecificAccessPolicy(entityId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE entity_payload
            SET specific_access_policy = null
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()

    internal fun getSpecificAccessPolicy(
        ngsiLdAttribute: NgsiLdAttribute
    ): Either<APIException, SpecificAccessPolicy> {
        val ngsiLdAttributeInstances = ngsiLdAttribute.getAttributeInstances()
        if (ngsiLdAttributeInstances.size > 1)
            return BadRequestDataException("Payload must contain a single attribute instance").left()
        val ngsiLdAttributeInstance = ngsiLdAttributeInstances[0]
        if (ngsiLdAttributeInstance !is NgsiLdPropertyInstance)
            return BadRequestDataException("Payload must be a property").left()
        return try {
            SpecificAccessPolicy.valueOf(ngsiLdAttributeInstance.value.toString()).right()
        } catch (e: java.lang.IllegalArgumentException) {
            BadRequestDataException("Value must be one of AUTH_READ or AUTH_WRITE (${e.message})").left()
        }
    }
}
