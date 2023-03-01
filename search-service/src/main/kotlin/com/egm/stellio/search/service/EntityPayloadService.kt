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
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
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
    private val entityAttributeCleanerService: EntityAttributeCleanerService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    suspend fun createEntity(
        payload: String,
        contexts: List<String>,
        sub: String? = null
    ): Either<APIException, Unit> = either {
        val jsonLdEntity = JsonLdUtils.expandJsonLdEntity(payload, contexts)
        val ngsiLdEntity = jsonLdEntity.toNgsiLdEntity()

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
        logger.debug("Creating entity ${ngsiLdEntity.id}")

        createEntityPayload(ngsiLdEntity, createdAt, jsonLdEntity).bind()
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
            serializeObject(jsonLdEntity.populateCreatedAt(createdAt).members),
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
        val filterQuery = buildEntitiesQueryFilter(
            queryParams,
            accessRightFilter
        ).let {
            if (queryParams.q != null)
                it.wrapToAndClause(buildQQuery(queryParams.q!!, queryParams.context))
            else it
        }

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
        val filterQuery = buildEntitiesQueryFilter(
            queryParams,
            accessRightFilter
        ).let {
            if (queryParams.q != null)
                it.plus(" AND (").plus(buildQQuery(queryParams.q!!, queryParams.context)).plus(")")
            else it
        }

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

    fun buildEntitiesQueryFilter(
        queryParams: QueryParams,
        accessRightFilter: () -> String?,
        prefix: String = ""
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
        val formattedTypes =
            if (queryParams.types.isNotEmpty())
                queryParams.types.joinToString(
                    separator = ",",
                    prefix = "entity_payload.types && ARRAY[",
                    postfix = "]"
                ) { "'$it'" }
            else null
        val formattedAttrs =
            if (queryParams.attrs.isNotEmpty())
                queryParams.attrs.joinToString(
                    separator = ",",
                    prefix = "attribute_name in (",
                    postfix = ")"
                ) { "'$it'" }
            else null

        val queryFilter =
            listOfNotNull(formattedIds, formattedIdPattern, formattedTypes, formattedAttrs, accessRightFilter())

        return if (queryFilter.isEmpty())
            queryFilter.joinToString(separator = " AND ")
        else
            queryFilter.joinToString(separator = " AND ", prefix = prefix)
    }

    private fun buildQQuery(rawQuery: String, context: String): String {
        val rawQueryWithPatternEscaped = rawQuery.escapeRegexpPattern()

        return rawQueryWithPatternEscaped.replace(qPattern.toRegex()) { matchResult ->
            // restoring the eventual inline options for regex expressions (replaced above)
            val fixedValue = matchResult.value.unescapeRegexPattern()

            val query = extractComparisonParametersFromQuery(fixedValue)

            val (mainAttributePath, trailingAttributePath) = query.first.parseAttributePath()
            val expandedAttribute = expandJsonLdTerm(mainAttributePath[0], context)
            val operator = query.second
            val value = query.third.prepareDateValue()

            when {
                mainAttributePath.size > 1 && !query.third.isURI() -> {
                    val expandedSubAttributePath = mainAttributePath.drop(1).map {
                        expandJsonLdTerm(it, context)
                    }.joinToString(".") { "\"$it\"" }
                    """
                    jsonb_path_exists(entity_payload.payload,
                        '$."$expandedAttribute".$expandedSubAttributePath.**{0 to 2}."$JSONLD_VALUE_KW" ? (@ $operator ${'$'}value)',
                        '{ "value": $value }')
                    """.trimIndent()
                }
                mainAttributePath.size > 1 && query.third.isURI() -> {
                    val expandSubAttribute = expandJsonLdTerm(mainAttributePath[1], context)
                    """
                    jsonb_path_exists(entity_payload.payload,
                        '$."$expandedAttribute"."$expandSubAttribute".**{0 to 2}."$JSONLD_ID" ? (@ $operator ${'$'}value)',
                        '{ "value": ${value.quote()} }')
                    """.trimIndent()
                }
                operator.isEmpty() ->
                    """
                    jsonb_path_exists(entity_payload.payload, '$."$expandedAttribute"')
                    """.trimIndent()
                trailingAttributePath.isNotEmpty() -> {
                    val expandedTrailingPaths = trailingAttributePath.map {
                        expandJsonLdTerm(it, context)
                    }.joinToString(".") { "\"$it\"" }
                    """
                    jsonb_path_exists(entity_payload.payload,
                        '$."$expandedAttribute"."$NGSILD_PROPERTY_VALUE".$expandedTrailingPaths.**{0 to 1}."$JSONLD_VALUE_KW" ? (@ $operator ${'$'}value)',
                        '{ "value": $value }')
                    """.trimIndent()
                }
                operator == "like_regex" ->
                    """
                    jsonb_path_exists(entity_payload.payload,
                        '$."$expandedAttribute"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW" ? (@ like_regex $value)')
                    """.trimIndent()
                operator == "not_like_regex" ->
                    """
                    NOT (jsonb_path_exists(entity_payload.payload,
                        '$."$expandedAttribute"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW" ? (@ like_regex $value)'))
                    """.trimIndent()
                value.isURI() ->
                    """
                    jsonb_path_exists(entity_payload.payload,
                        '$."$expandedAttribute"."$NGSILD_RELATIONSHIP_HAS_OBJECT"."$JSONLD_ID" ? (@ $operator ${'$'}value)',
                        '{ "value": ${value.quote()} }')
                    """.trimIndent()
                value.isRange() -> {
                    val (min, max) = query.third.rangeInterval()
                    """
                    jsonb_path_exists(entity_payload.payload,
                        '$."$expandedAttribute"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW" ? (@ >= ${'$'}min && @ <= ${'$'}max)',
                        '{ "min": $min, "max": $max }')
                    """.trimIndent()
                }
                value.isValueList() -> {
                    // can't use directly the || operator since it will be replaced below when composing the filters
                    // so use an intermediate JSONPATH_OR_FILTER placeholder
                    val valuesFilter = query.third.listOfValues()
                        .joinToString(separator = " JSONPATH_OR_FILTER ") { "@ == $it" }
                    """
                    jsonb_path_exists(entity_payload.payload,
                        '$."$expandedAttribute"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW" ? ($valuesFilter)')
                    """.trimIndent()
                }
                else ->
                    """
                    jsonb_path_exists(entity_payload.payload,
                        '$."$expandedAttribute"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW" ? (@ $operator ${'$'}value)',
                        '{ "value": $value }')
                    """.trimIndent()
            }
        }
            .replace(";", " AND ")
            .replace("|", " OR ")
            .replace("JSONPATH_OR_FILTER", "||")
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
                .bind("modified_at", ZonedDateTime.now(ZoneOffset.UTC))
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
        ngsiLdAttributes: List<NgsiLdAttribute>,
        jsonLdAttributes: Map<String, Any>,
        disallowOverwrite: Boolean,
        sub: Sub?
    ): Either<APIException, UpdateResult> =
        either {
            val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
            val updateResult = temporalEntityAttributeService.appendEntityAttributes(
                entityUri,
                ngsiLdAttributes,
                jsonLdAttributes,
                disallowOverwrite,
                createdAt,
                sub
            ).bind()
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
        ngsiLdAttributes: List<NgsiLdAttribute>,
        jsonLdAttributes: Map<String, Any>,
        sub: Sub?
    ): Either<APIException, UpdateResult> =
        either {
            val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
            val updateResult = temporalEntityAttributeService.updateEntityAttributes(
                entityUri,
                ngsiLdAttributes,
                jsonLdAttributes,
                createdAt,
                sub
            ).bind()
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
        expandedPayload: Map<String, List<Map<String, List<Any>>>>,
        sub: Sub?
    ): Either<APIException, UpdateResult> =
        either {
            val modifiedAt = ZonedDateTime.now(ZoneOffset.UTC)
            val updateResult = temporalEntityAttributeService.partialUpdateEntityAttribute(
                entityId,
                expandedPayload,
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
        jsonLdInstances: ExpandedAttributesInstances,
        sub: Sub?
    ): Either<APIException, Unit> =
        either {
            val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
            jsonLdInstances.forEach { (attributeName, expandedAttributeInstances) ->
                expandedAttributeInstances.forEach { expandedAttributeInstance ->
                    val jsonLdAttribute = mapOf(attributeName to listOf(expandedAttributeInstance))
                    val ngsiLdAttribute = parseAttributesInstancesToNgsiLdAttributes(jsonLdAttribute)[0]

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
    suspend fun deleteEntityPayload(entityId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM entity_payload WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()
            .onRight {
                entityAttributeCleanerService.deleteEntityAttributes(entityId)
            }

    @Transactional
    suspend fun deleteAttribute(
        entityId: URI,
        attributeName: String,
        datasetId: URI?,
        deleteAll: Boolean = false
    ): Either<APIException, Unit> =
        either {
            temporalEntityAttributeService.deleteTemporalAttribute(
                entityId,
                attributeName,
                datasetId,
                deleteAll
            ).bind()
            updateState(
                entityId,
                ZonedDateTime.now(ZoneOffset.UTC),
                temporalEntityAttributeService.getForEntity(entityId, emptySet())
            ).bind()
        }

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
