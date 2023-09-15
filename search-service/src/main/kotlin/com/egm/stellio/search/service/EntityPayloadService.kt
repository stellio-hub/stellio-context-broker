package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.model.OperationType.*
import com.egm.stellio.search.scope.ScopeService
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_EXPANDED_ENTITY_SPECIFIC_MEMBERS
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_PROPERTY
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import io.r2dbc.postgresql.codec.Json
import org.slf4j.LoggerFactory
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.ZoneOffset
import java.time.ZonedDateTime

@Service
class EntityPayloadService(
    private val databaseClient: DatabaseClient,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val scopeService: ScopeService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    suspend fun createEntity(
        payload: String,
        contexts: List<String>,
        sub: String? = null
    ): Either<APIException, Unit> = either {
        val jsonLdEntity = JsonLdUtils.expandJsonLdEntity(payload, contexts)
        val ngsiLdEntity = jsonLdEntity.toNgsiLdEntity().bind()

        createEntity(ngsiLdEntity, jsonLdEntity, sub).bind()
    }

    @Transactional
    suspend fun createEntity(
        ngsiLdEntity: NgsiLdEntity,
        jsonLdEntity: JsonLdEntity,
        sub: String? = null
    ): Either<APIException, Unit> = either {
        val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
        val attributesMetadata = ngsiLdEntity.prepareTemporalAttributes().bind()
        logger.debug("Creating entity {}", ngsiLdEntity.id)

        createEntityPayload(ngsiLdEntity, jsonLdEntity, createdAt, sub = sub).bind()
        temporalEntityAttributeService.createEntityTemporalReferences(
            ngsiLdEntity,
            jsonLdEntity,
            attributesMetadata,
            createdAt,
            sub
        ).bind()
    }

    @Transactional
    suspend fun createEntityPayload(
        ngsiLdEntity: NgsiLdEntity,
        jsonLdEntity: JsonLdEntity,
        createdAt: ZonedDateTime,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        val specificAccessPolicy = ngsiLdEntity.getSpecificAccessPolicy()?.bind()
        databaseClient.sql(
            """
            INSERT INTO entity_payload (entity_id, types, scopes, created_at, payload, contexts, specific_access_policy)
            VALUES (:entity_id, :types, :scopes, :created_at, :payload, :contexts, :specific_access_policy)
            """.trimIndent()
        )
            .bind("entity_id", ngsiLdEntity.id)
            .bind("types", ngsiLdEntity.types.toTypedArray())
            .bind("scopes", ngsiLdEntity.scopes?.toTypedArray())
            .bind("created_at", createdAt)
            .bind("payload", Json.of(serializeObject(jsonLdEntity.populateCreationTimeDate(createdAt).members)))
            .bind("contexts", ngsiLdEntity.contexts.toTypedArray())
            .bind("specific_access_policy", specificAccessPolicy?.toString())
            .execute()
            .map {
                scopeService.createHistory(ngsiLdEntity, createdAt, sub)
            }
    }

    @Transactional
    suspend fun mergeEntity(
        entityId: URI,
        expandedAttributes: ExpandedAttributes,
        observedAt: ZonedDateTime?,
        sub: Sub?
    ): Either<APIException, UpdateResult> = either {
        logger.debug("Merging entity {}", entityId)

        val (coreAttrs, otherAttrs) =
            expandedAttributes.toList().partition { JSONLD_EXPANDED_ENTITY_SPECIFIC_MEMBERS.contains(it.first) }
        val mergedAt = ngsiLdDateTime()

        val coreUpdateResult = updateCoreAttributes(entityId, coreAttrs, mergedAt, MERGE_ENTITY).bind()
        val attrsUpdateResult = temporalEntityAttributeService.mergeEntityAttributes(
            entityId,
            otherAttrs.toMap().toNgsiLdAttributes().bind(),
            expandedAttributes,
            mergedAt,
            observedAt,
            sub
        ).bind()

        val updateResult = coreUpdateResult.mergeWith(attrsUpdateResult)
        // update modifiedAt in entity if at least one attribute has been merged
        if (updateResult.hasSuccessfulUpdate()) {
            val teas = temporalEntityAttributeService.getForEntity(entityId, emptySet())
            updateState(entityId, mergedAt, teas).bind()
        }
        updateResult
    }

    @Transactional
    suspend fun replaceEntity(
        entityId: URI,
        ngsiLdEntity: NgsiLdEntity,
        jsonLdEntity: JsonLdEntity,
        sub: String? = null
    ): Either<APIException, Unit> = either {
        val replacedAt = ngsiLdDateTime()
        val attributesMetadata = ngsiLdEntity.prepareTemporalAttributes().bind()
        logger.debug("Replacing entity {}", ngsiLdEntity.id)

        temporalEntityAttributeService.deleteTemporalAttributesOfEntity(entityId)

        replaceEntityPayload(ngsiLdEntity, jsonLdEntity, replacedAt, sub).bind()
        temporalEntityAttributeService.createEntityTemporalReferences(
            ngsiLdEntity,
            jsonLdEntity,
            attributesMetadata,
            replacedAt,
            sub
        ).bind()
    }

    @Transactional
    suspend fun replaceEntityPayload(
        ngsiLdEntity: NgsiLdEntity,
        jsonLdEntity: JsonLdEntity,
        replacedAt: ZonedDateTime,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        val specificAccessPolicy = ngsiLdEntity.getSpecificAccessPolicy()?.bind()
        val createdAt = retrieveCreatedAt(ngsiLdEntity.id).bind()
        val serializedPayload =
            serializeObject(jsonLdEntity.populateReplacementTimeDates(createdAt, replacedAt).members)

        databaseClient.sql(
            """
            UPDATE entity_payload
            SET types = :types,
                scopes = :scopes,
                modified_at = :modified_at,
                payload = :payload,
                specific_access_policy = :specific_access_policy,
                contexts = :contexts
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", ngsiLdEntity.id)
            .bind("types", ngsiLdEntity.types.toTypedArray())
            .bind("scopes", ngsiLdEntity.scopes?.toTypedArray())
            .bind("modified_at", replacedAt)
            .bind("payload", Json.of(serializedPayload))
            .bind("contexts", jsonLdEntity.contexts.toTypedArray())
            .bind("specific_access_policy", specificAccessPolicy?.toString())
            .execute()
            .map {
                scopeService.replaceHistoryEntry(ngsiLdEntity, createdAt, sub)
            }
    }

    suspend fun retrieveCreatedAt(entityId: URI): Either<APIException, ZonedDateTime> =
        databaseClient.sql(
            """
            SELECT created_at from entity_payload
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .oneToResult { toZonedDateTime(it["created_at"]) }

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
            scopes = toOptionalList(row["scopes"]),
            createdAt = toZonedDateTime(row["created_at"]),
            modifiedAt = toOptionalZonedDateTime(row["modified_at"]),
            contexts = toList(row["contexts"]),
            payload = toJson(row["payload"]),
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

    suspend fun queryEntities(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): List<URI> {
        val filterQuery = buildFullEntitiesFilter(queryParams, accessRightFilter)

        val selectQuery =
            """
            SELECT DISTINCT(entity_payload.entity_id)
            FROM entity_payload
            LEFT JOIN temporal_entity_attribute tea
            ON tea.entity_id = entity_payload.entity_id
            WHERE $filterQuery
            ORDER BY entity_id
            LIMIT :limit
            OFFSET :offset   
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("limit", queryParams.limit)
            .bind("offset", queryParams.offset)
            .allToMappedList { toUri(it["entity_id"]) }
    }

    suspend fun queryEntitiesCount(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): Either<APIException, Int> {
        val filterQuery = buildFullEntitiesFilter(queryParams, accessRightFilter)

        val countQuery =
            """
            SELECT count(distinct(entity_payload.entity_id)) as count_entity
            FROM entity_payload
            LEFT JOIN temporal_entity_attribute tea
            ON tea.entity_id = entity_payload.entity_id
            WHERE $filterQuery
            """.trimIndent()

        return databaseClient
            .sql(countQuery)
            .oneToResult { it["count_entity"] as Long }
            .map { it.toInt() }
    }

    private fun buildFullEntitiesFilter(queryParams: QueryParams, accessRightFilter: () -> String?): String =
        buildEntitiesQueryFilter(
            queryParams,
            accessRightFilter
        ).let {
            if (queryParams.q != null)
                it.wrapToAndClause(buildQQuery(queryParams.q!!, listOf(queryParams.context)))
            else it
        }.let {
            if (queryParams.scopeQ != null)
                it.wrapToAndClause(buildScopeQQuery(queryParams.scopeQ!!))
            else it
        }.let {
            if (queryParams.geoQuery != null)
                it.wrapToAndClause(buildGeoQuery(queryParams.geoQuery!!))
            else it
        }

    fun buildEntitiesQueryFilter(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): String {
        val formattedIds =
            if (queryParams.ids.isNotEmpty())
                queryParams.ids.joinToString(
                    separator = ",",
                    prefix = "entity_payload.entity_id in(",
                    postfix = ")"
                ) { "'$it'" }
            else null
        val formattedIdPattern =
            if (!queryParams.idPattern.isNullOrEmpty())
                "entity_payload.entity_id ~ '${queryParams.idPattern}'"
            else null
        val formattedType = queryParams.type?.let { buildTypeQuery(it) }
        val formattedAttrs =
            if (queryParams.attrs.isNotEmpty())
                queryParams.attrs.joinToString(
                    separator = ",",
                    prefix = "attribute_name in (",
                    postfix = ")"
                ) { "'$it'" }
            else null

        val queryFilter =
            listOfNotNull(formattedIds, formattedIdPattern, formattedType, formattedAttrs, accessRightFilter())

        return queryFilter.joinToString(separator = " AND ")
    }

    suspend fun hasSpecificAccessPolicies(
        entityId: URI,
        specificAccessPolicies: List<SpecificAccessPolicy>
    ): Either<APIException, Boolean> {
        if (specificAccessPolicies.isEmpty())
            return either { false }

        return databaseClient.sql(
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
    }

    suspend fun filterExistingEntitiesAsIds(entitiesIds: List<URI>): List<URI> {
        if (entitiesIds.isEmpty()) {
            return emptyList()
        }

        val query =
            """
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
    suspend fun updateCoreAttributes(
        entityId: URI,
        coreAttrs: List<Pair<ExpandedTerm, ExpandedAttributeInstances>>,
        modifiedAt: ZonedDateTime,
        operationType: OperationType
    ): Either<APIException, UpdateResult> = either {
        coreAttrs.map { (expandedTerm, expandedAttributeInstances) ->
            when (expandedTerm) {
                JSONLD_TYPE ->
                    updateTypes(entityId, expandedAttributeInstances as List<ExpandedTerm>, modifiedAt).bind()
                NGSILD_SCOPE_PROPERTY ->
                    scopeService.update(entityId, expandedAttributeInstances, modifiedAt, operationType).bind()
                else -> {
                    logger.warn("Ignoring unhandled core property: {}", expandedTerm)
                    EMPTY_UPDATE_RESULT.right().bind()
                }
            }
        }.ifEmpty { listOf(EMPTY_UPDATE_RESULT) }
            .reduce { acc, cur -> acc.mergeWith(cur) }
    }

    @Transactional
    suspend fun updateTypes(
        entityId: URI,
        newTypes: List<ExpandedTerm>,
        modifiedAt: ZonedDateTime,
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
                            attributeName = JSONLD_TYPE,
                            updateOperationResult = UpdateOperationResult.FAILED,
                            errorMessage = "A type cannot be removed from an entity: $removedTypes have been removed"
                        )
                    )
                )
            }

            val updatedPayload = entityPayload.payload.deserializeExpandedPayload()
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
                .bind("modified_at", modifiedAt)
                .bind("payload", Json.of(serializeObject(updatedPayload)))
                .execute()
                .map {
                    updateResultFromDetailedResult(
                        listOf(
                            UpdateAttributeResult(
                                attributeName = JSONLD_TYPE,
                                updateOperationResult = UpdateOperationResult.APPENDED
                            )
                        )
                    )
                }.bind()
        }

    @Transactional
    suspend fun appendAttributes(
        entityUri: URI,
        expandedAttributes: ExpandedAttributes,
        disallowOverwrite: Boolean,
        sub: Sub?
    ): Either<APIException, UpdateResult> =
        either {
            val (coreAttrs, otherAttrs) =
                expandedAttributes.toList().partition { JSONLD_EXPANDED_ENTITY_SPECIFIC_MEMBERS.contains(it.first) }
            val createdAt = ZonedDateTime.now(ZoneOffset.UTC)

            val operationType =
                if (disallowOverwrite) APPEND_ATTRIBUTES
                else APPEND_ATTRIBUTES_OVERWRITE_ALLOWED
            val coreUpdateResult = updateCoreAttributes(entityUri, coreAttrs, createdAt, operationType).bind()
            val attrsUpdateResult = temporalEntityAttributeService.appendEntityAttributes(
                entityUri,
                otherAttrs.toMap().toNgsiLdAttributes().bind(),
                expandedAttributes,
                disallowOverwrite,
                createdAt,
                sub
            ).bind()

            val updateResult = coreUpdateResult.mergeWith(attrsUpdateResult)
            // update modifiedAt in entity if at least one attribute has been added
            if (updateResult.hasSuccessfulUpdate()) {
                val teas = temporalEntityAttributeService.getForEntity(entityUri, emptySet())
                updateState(entityUri, createdAt, teas).bind()
            }
            updateResult
        }

    @Transactional
    suspend fun updateAttributes(
        entityUri: URI,
        expandedAttributes: ExpandedAttributes,
        sub: Sub?
    ): Either<APIException, UpdateResult> =
        either {
            val (coreAttrs, otherAttrs) =
                expandedAttributes.toList().partition { JSONLD_EXPANDED_ENTITY_SPECIFIC_MEMBERS.contains(it.first) }
            val createdAt = ZonedDateTime.now(ZoneOffset.UTC)

            val coreUpdateResult = updateCoreAttributes(entityUri, coreAttrs, createdAt, UPDATE_ATTRIBUTES).bind()
            val attrsUpdateResult = temporalEntityAttributeService.updateEntityAttributes(
                entityUri,
                otherAttrs.toMap().toNgsiLdAttributes().bind(),
                expandedAttributes,
                createdAt,
                sub
            ).bind()

            val updateResult = coreUpdateResult.mergeWith(attrsUpdateResult)
            // update modifiedAt in entity if at least one attribute has been added
            if (updateResult.hasSuccessfulUpdate()) {
                val teas = temporalEntityAttributeService.getForEntity(entityUri, emptySet())
                updateState(entityUri, createdAt, teas).bind()
            }
            updateResult
        }

    @Transactional
    suspend fun partialUpdateAttribute(
        entityId: URI,
        expandedAttribute: ExpandedAttribute,
        sub: Sub?
    ): Either<APIException, UpdateResult> =
        either {
            val modifiedAt = ZonedDateTime.now(ZoneOffset.UTC)
            val updateResult = temporalEntityAttributeService.partialUpdateEntityAttribute(
                entityId,
                expandedAttribute,
                modifiedAt,
                sub
            ).bind()
            if (updateResult.isSuccessful()) {
                val teas = temporalEntityAttributeService.getForEntity(entityId, emptySet())
                updateState(entityId, modifiedAt, teas).bind()
            }
            updateResult
        }

    @Transactional
    suspend fun upsertAttributes(
        entityId: URI,
        expandedAttributes: ExpandedAttributes,
        sub: Sub?
    ): Either<APIException, Unit> =
        either {
            val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
            expandedAttributes.forEach { (attributeName, expandedAttributeInstances) ->
                expandedAttributeInstances.forEach { expandedAttributeInstance ->
                    val jsonLdAttribute = mapOf(attributeName to listOf(expandedAttributeInstance))
                    val ngsiLdAttribute = jsonLdAttribute.toNgsiLdAttributes().bind()[0]

                    temporalEntityAttributeService.upsertEntityAttributes(
                        entityId,
                        ngsiLdAttribute,
                        jsonLdAttribute,
                        createdAt,
                        sub
                    ).bind()
                }
            }
            updateState(entityId, createdAt, temporalEntityAttributeService.getForEntity(entityId, emptySet())).bind()
        }

    @Transactional
    suspend fun replaceAttribute(
        entityId: URI,
        expandedAttribute: ExpandedAttribute,
        sub: Sub?
    ): Either<APIException, UpdateResult> = either {
        val ngsiLdAttribute = listOf(expandedAttribute).toMap().toNgsiLdAttributes().bind()[0]
        val replacedAt = ngsiLdDateTime()

        val updateResult = temporalEntityAttributeService.replaceEntityAttribute(
            entityId,
            ngsiLdAttribute,
            expandedAttribute,
            replacedAt,
            sub
        ).bind()

        // update modifiedAt in entity if at least one attribute has been added
        if (updateResult.hasSuccessfulUpdate()) {
            val teas = temporalEntityAttributeService.getForEntity(entityId, emptySet())
            updateState(entityId, replacedAt, teas).bind()
        }
        updateResult
    }

    @Transactional
    suspend fun updateState(
        entityUri: URI,
        modifiedAt: ZonedDateTime,
        temporalEntityAttributes: List<TemporalEntityAttribute>
    ): Either<APIException, Unit> =
        retrieve(entityUri)
            .map { entityPayload ->
                val payload = buildJsonLdEntity(
                    temporalEntityAttributes,
                    entityPayload.copy(modifiedAt = modifiedAt)
                )
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
            .groupBy { tea ->
                tea.attributeName
            }
            .mapValues { (_, teas) ->
                teas.map { tea ->
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
    suspend fun deleteEntity(entityId: URI): Either<APIException, Unit> = either {
        databaseClient.sql(
            """
            DELETE FROM entity_payload WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()
            .bind()

        temporalEntityAttributeService.deleteTemporalAttributesOfEntity(entityId).bind()
        scopeService.deleteHistory(entityId).bind()
    }

    @Transactional
    suspend fun deleteAttribute(
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI?,
        deleteAll: Boolean = false
    ): Either<APIException, Unit> = either {
        when (attributeName) {
            NGSILD_SCOPE_PROPERTY -> scopeService.delete(entityId).bind()
            else -> {
                temporalEntityAttributeService.checkEntityAndAttributeExistence(
                    entityId,
                    attributeName,
                    datasetId
                ).bind()
                temporalEntityAttributeService.deleteTemporalAttribute(
                    entityId,
                    attributeName,
                    datasetId,
                    deleteAll
                ).bind()
            }
        }
        updateState(
            entityId,
            ngsiLdDateTime(),
            temporalEntityAttributeService.getForEntity(entityId, emptySet())
        ).bind()
    }

    suspend fun updateSpecificAccessPolicy(
        entityId: URI,
        ngsiLdAttribute: NgsiLdAttribute
    ): Either<APIException, Unit> = either {
        val specificAccessPolicy = ngsiLdAttribute.getSpecificAccessPolicy().bind()
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
            .bind()
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
}
