package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import arrow.fx.coroutines.parTraverseEither
import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonLdUtils.getAttributeFromExpandedAttributes
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.savvasdalkitsis.jsonmerger.JsonMerger
import io.r2dbc.postgresql.codec.Json
import org.json.JSONObject
import org.slf4j.LoggerFactory
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID
import java.util.regex.Pattern

@Service
class TemporalEntityAttributeService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val attributeInstanceService: AttributeInstanceService,
    private val entityPayloadService: EntityPayloadService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    suspend fun create(temporalEntityAttribute: TemporalEntityAttribute): Either<APIException, Unit> =
        databaseClient.sql(
            """
            INSERT INTO temporal_entity_attribute
                (id, entity_id, attribute_name, attribute_type, attribute_value_type, created_at, dataset_id, 
                    payload)
            VALUES 
                (:id, :entity_id, :attribute_name, :attribute_type, :attribute_value_type, :created_at, :dataset_id, 
                    :payload)
            """.trimIndent()
        )
            .bind("id", temporalEntityAttribute.id)
            .bind("entity_id", temporalEntityAttribute.entityId)
            .bind("attribute_name", temporalEntityAttribute.attributeName)
            .bind("attribute_type", temporalEntityAttribute.attributeType.toString())
            .bind("attribute_value_type", temporalEntityAttribute.attributeValueType.toString())
            .bind("created_at", temporalEntityAttribute.createdAt)
            .bind("dataset_id", temporalEntityAttribute.datasetId)
            .bind("payload", Json.of(temporalEntityAttribute.payload))
            .execute()

    @Transactional
    suspend fun update(
        teaUUID: UUID,
        attributeMetadata: AttributeMetadata,
        modifiedAt: ZonedDateTime,
        payload: String
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET 
                attribute_type = :attribute_type,
                attribute_value_type = :attribute_value_type,
                modified_at = :modified_at,
                payload = :payload
            WHERE id = :id
            """.trimIndent()
        )
            .bind("id", teaUUID)
            .bind("attribute_type", attributeMetadata.type.toString())
            .bind("attribute_value_type", attributeMetadata.valueType.toString())
            .bind("modified_at", modifiedAt)
            .bind("payload", Json.of(payload))
            .execute()

    @Transactional
    suspend fun createEntityTemporalReferences(
        payload: String,
        contexts: List<String>,
        sub: String? = null
    ): Either<APIException, Unit> {
        val jsonLdEntity = expandJsonLdEntity(payload, contexts)
        val ngsiLdEntity = jsonLdEntity.toNgsiLdEntity()
        return ngsiLdEntity.prepareTemporalAttributes()
            .map {
                createEntityTemporalReferences(
                    jsonLdEntity.toNgsiLdEntity(),
                    jsonLdEntity,
                    it,
                    sub
                )
            }
    }

    @Transactional
    suspend fun createEntityTemporalReferences(
        ngsiLdEntity: NgsiLdEntity,
        jsonLdEntity: JsonLdEntity,
        attributesMetadata: List<Pair<ExpandedTerm, AttributeMetadata>>,
        sub: String? = null
    ): Either<APIException, Unit> = either {
        val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
        logger.debug("Creating ${attributesMetadata.size} attributes in entity: ${ngsiLdEntity.id}")

        entityPayloadService.createEntityPayload(ngsiLdEntity, createdAt, jsonLdEntity).bind()

        attributesMetadata
            .filter {
                it.first != AuthContextModel.AUTH_PROP_SAP
            }
            .forEach {
                val (expandedAttributeName, attributeMetadata) = it
                val attributePayload = getAttributeFromExpandedAttributes(
                    jsonLdEntity.properties,
                    expandedAttributeName,
                    attributeMetadata.datasetId
                )!!
                val temporalEntityAttribute = TemporalEntityAttribute(
                    entityId = ngsiLdEntity.id,
                    attributeName = expandedAttributeName,
                    attributeType = attributeMetadata.type,
                    attributeValueType = attributeMetadata.valueType,
                    datasetId = attributeMetadata.datasetId,
                    createdAt = createdAt,
                    payload = serializeObject(attributePayload)
                )
                create(temporalEntityAttribute).bind()

                val attributeCreatedAtInstance = AttributeInstance(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
                    time = createdAt,
                    measuredValue = attributeMetadata.measuredValue,
                    value = attributeMetadata.value,
                    geoValue = attributeMetadata.geoValue,
                    payload = attributePayload,
                    sub = sub
                )
                attributeInstanceService.create(attributeCreatedAtInstance).bind()

                if (attributeMetadata.observedAt != null) {
                    val attributeObservedAtInstance = attributeCreatedAtInstance.copy(
                        time = attributeMetadata.observedAt,
                        timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
                    )
                    attributeInstanceService.create(attributeObservedAtInstance).bind()
                }
            }
    }

    @Transactional
    suspend fun updateStatus(
        teaUUID: UUID,
        modifiedAt: ZonedDateTime,
        payload: ExpandedAttributePayloadEntry
    ): Either<APIException, Unit> =
        updateStatus(teaUUID, modifiedAt, serializeObject(payload))

    @Transactional
    suspend fun updateStatus(
        teaUUID: UUID,
        modifiedAt: ZonedDateTime,
        payload: String
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET payload = :payload,
                modified_at = :modified_at
            WHERE id = :tea_uuid
            """.trimIndent()
        )
            .bind("tea_uuid", teaUUID)
            .bind("payload", Json.of(payload))
            .bind("modified_at", modifiedAt)
            .execute()

    suspend fun addAttribute(
        entityId: URI,
        ngsiLdAttribute: NgsiLdAttribute,
        attributeMetadata: AttributeMetadata,
        createdAt: ZonedDateTime,
        attributePayload: ExpandedAttributePayloadEntry,
        sub: Sub?
    ): Either<APIException, Unit> =
        either {
            logger.debug("Adding attribute ${ngsiLdAttribute.name} to entity $entityId")
            val temporalEntityAttribute = TemporalEntityAttribute(
                entityId = entityId,
                attributeName = ngsiLdAttribute.name,
                attributeType = attributeMetadata.type,
                attributeValueType = attributeMetadata.valueType,
                datasetId = attributeMetadata.datasetId,
                createdAt = createdAt,
                payload = serializeObject(attributePayload)
            )
            create(temporalEntityAttribute).bind()

            val attributeInstance = AttributeInstance(
                temporalEntityAttribute = temporalEntityAttribute.id,
                timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
                time = createdAt,
                measuredValue = attributeMetadata.measuredValue,
                value = attributeMetadata.value,
                geoValue = attributeMetadata.geoValue,
                payload = attributePayload,
                sub = sub
            )
            attributeInstanceService.create(attributeInstance).bind()

            if (attributeMetadata.observedAt != null) {
                val attributeObservedAtInstance = attributeInstance.copy(
                    time = attributeMetadata.observedAt,
                    timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
                )
                attributeInstanceService.create(attributeObservedAtInstance).bind()
            }
        }

    suspend fun replaceAttribute(
        temporalEntityAttribute: TemporalEntityAttribute,
        ngsiLdAttribute: NgsiLdAttribute,
        attributeMetadata: AttributeMetadata,
        createdAt: ZonedDateTime,
        attributePayload: ExpandedAttributePayloadEntry,
        sub: Sub?
    ): Either<APIException, Unit> =
        either {
            logger.debug(
                "Replacing attribute {} ({}) in entity {}",
                ngsiLdAttribute.name,
                attributeMetadata.datasetId,
                temporalEntityAttribute.entityId
            )
            update(temporalEntityAttribute.id, attributeMetadata, createdAt, serializeObject(attributePayload)).bind()

            val attributeInstance = AttributeInstance(
                temporalEntityAttribute = temporalEntityAttribute.id,
                timeProperty = AttributeInstance.TemporalProperty.MODIFIED_AT,
                time = createdAt,
                measuredValue = attributeMetadata.measuredValue,
                value = attributeMetadata.value,
                geoValue = attributeMetadata.geoValue,
                payload = attributePayload,
                sub = sub
            )
            attributeInstanceService.create(attributeInstance).bind()

            if (attributeMetadata.observedAt != null) {
                val attributeObservedAtInstance = attributeInstance.copy(
                    time = attributeMetadata.observedAt,
                    timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
                )
                attributeInstanceService.create(attributeObservedAtInstance).bind()
            }
        }

    suspend fun deleteTemporalEntityReferences(entityId: URI): Either<APIException, Unit> =
        either {
            logger.debug("Deleting entity $entityId")
            attributeInstanceService.deleteInstancesOfEntity(entityId).bind()
            deleteTemporalAttributesOfEntity(entityId).bind()
            entityPayloadService.deleteEntityPayload(entityId).bind()
        }

    suspend fun deleteTemporalAttributesOfEntity(entityId: URI): Either<APIException, Unit> =
        r2dbcEntityTemplate.delete(TemporalEntityAttribute::class.java)
            .matching(query(where("entity_id").`is`(entityId)))
            .execute()

    suspend fun deleteTemporalAttribute(
        entityId: URI,
        attributeName: String,
        datasetId: URI?,
        deleteAll: Boolean = false
    ): Either<APIException, Unit> =
        either {
            logger.debug("Deleting attribute $attributeName from entity $entityId (all: $deleteAll)")
            if (deleteAll) {
                attributeInstanceService.deleteAllInstancesOfAttribute(entityId, attributeName).bind()
                deleteTemporalAttributeAllInstancesReferences(entityId, attributeName).bind()
            } else {
                attributeInstanceService.deleteInstancesOfAttribute(entityId, attributeName, datasetId).bind()
                deleteTemporalAttributeReferences(entityId, attributeName, datasetId).bind()
            }
            entityPayloadService.updateState(
                entityId,
                ZonedDateTime.now(ZoneOffset.UTC),
                getForEntity(entityId, emptySet())
            ).bind()
        }

    @Transactional
    suspend fun deleteTemporalAttributeReferences(
        entityId: URI,
        attributeName: String,
        datasetId: URI?
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            ${datasetId.toDatasetIdFilter()}
            AND attribute_name = :attribute_name
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("attribute_name", attributeName)
            .let {
                if (datasetId != null) it.bind("dataset_id", datasetId)
                else it
            }
            .execute()

    @Transactional
    suspend fun deleteTemporalAttributeAllInstancesReferences(
        entityId: URI,
        attributeName: String
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            AND attribute_name = :attribute_name
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("attribute_name", attributeName)
            .execute()

    suspend fun getForEntities(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): List<URI> {
        val filterQuery = buildEntitiesQueryFilter(
            queryParams,
            accessRightFilter
        ).let {
            if (queryParams.q != null)
                it.wrapToAndClause(buildInnerQuery(queryParams.q!!, queryParams.context))
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

    suspend fun getForTemporalEntities(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): List<TemporalEntityAttribute> {
        val filterQuery = buildEntitiesQueryFilter(
            queryParams,
            accessRightFilter
        ).let {
            if (queryParams.q != null)
                it.wrapToAndClause(buildInnerQuery(queryParams.q!!, queryParams.context))
            else it
        }

        val filterOnAttributesQuery = buildEntitiesQueryFilter(
            queryParams.copy(ids = emptySet(), idPattern = null, types = emptySet()),
            { null },
            " AND "
        )

        val selectQuery =
            """
                WITH entities AS (
                    SELECT DISTINCT(tea.entity_id)
                    FROM temporal_entity_attribute tea
                    JOIN entity_payload ON tea.entity_id = entity_payload.entity_id
                    WHERE $filterQuery
                    ORDER BY entity_id
                    LIMIT :limit
                    OFFSET :offset   
                )
                SELECT id, entity_id, attribute_name, attribute_type, attribute_value_type, created_at, modified_at,
                    dataset_id, payload
                FROM temporal_entity_attribute            
                WHERE entity_id IN (SELECT entity_id FROM entities) 
                $filterOnAttributesQuery
                ORDER BY entity_id
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("limit", queryParams.limit)
            .bind("offset", queryParams.offset)
            .allToMappedList { rowToTemporalEntityAttribute(it) }
    }

    suspend fun getCountForEntities(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): Either<APIException, Int> {
        val filterQuery = buildEntitiesQueryFilter(
            queryParams,
            accessRightFilter
        ).let {
            if (queryParams.q != null)
                it.plus(" AND (").plus(buildInnerQuery(queryParams.q!!, queryParams.context)).plus(")")
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

    private val innerRegexPattern: Pattern = Pattern.compile(".*(=~\"\\(\\?i\\)).*")

    private fun buildInnerQuery(rawQuery: String, context: String): String {
        // Quick hack to allow inline options for regex expressions
        // (see https://keith.github.io/xcode-man-pages/re_format.7.html for more details)
        // When matched, parenthesis are replaced by special characters that are later restored after the main
        // qPattern regex has been processed
        val rawQueryWithPatternEscaped =
            if (rawQuery.matches(innerRegexPattern.toRegex())) {
                rawQuery.replace(innerRegexPattern.toRegex()) { matchResult ->
                    matchResult.value
                        .replace("(", "##")
                        .replace(")", "//")
                }
            } else rawQuery

        return rawQueryWithPatternEscaped.replace(qPattern.toRegex()) { matchResult ->
            // restoring the eventual inline options for regex expressions (replaced above)
            val fixedValue = matchResult.value
                .replace("##", "(")
                .replace("//", ")")
            val query = extractComparisonParametersFromQuery(fixedValue)
            val targetValue = query.third.prepareDateValue(query.second)
            """
            EXISTS(
               SELECT 1
               FROM temporal_entity_attribute
               WHERE tea.entity_id = temporal_entity_attribute.entity_id
               AND (attribute_name = '${expandJsonLdTerm(query.first, listOf(context))}'
                    AND CASE 
                        WHEN attribute_type = 'Property' AND attribute_value_type IN ('DATETIME', 'DATE', 'TIME') THEN
                            CASE 
                                WHEN '${query.second}' != 'like_regex' THEN
                                    jsonb_path_exists(temporal_entity_attribute.payload,
                                        '$."$NGSILD_PROPERTY_VALUE" ? 
                                            (@."$JSONLD_VALUE_KW".datetime($DATETIME_TEMPLATE) ${query.second} $targetValue)')
                            END
                        WHEN attribute_type = 'Property' THEN
                            jsonb_path_exists(temporal_entity_attribute.payload,
                                '$."$NGSILD_PROPERTY_VALUE" ? 
                                    (@."$JSONLD_VALUE_KW" ${query.second} $targetValue)')
                        WHEN attribute_type = 'Relationship' THEN
                            jsonb_path_exists(temporal_entity_attribute.payload,
                                '$."$NGSILD_RELATIONSHIP_HAS_OBJECT" ? 
                                    (@."$JSONLD_ID" ${query.second} $targetValue)')
                        END
                        
               )
            )
            """.trimIndent()
        }
            .replace(";", " AND ")
            .replace("|", " OR ")
    }

    suspend fun getForEntity(id: URI, attrs: Set<String>): List<TemporalEntityAttribute> {
        val selectQuery =
            """
                SELECT id, entity_id, attribute_name, attribute_type, attribute_value_type, created_at, modified_at, 
                    dataset_id, payload
                FROM temporal_entity_attribute            
                WHERE entity_id = :entity_id
            """.trimIndent()

        val expandedAttrsList = attrs.joinToString(",") { "'$it'" }
        val finalQuery =
            if (attrs.isNotEmpty())
                "$selectQuery AND attribute_name in ($expandedAttrsList)"
            else
                selectQuery

        return databaseClient
            .sql(finalQuery)
            .bind("entity_id", id)
            .allToMappedList { rowToTemporalEntityAttribute(it) }
    }

    suspend fun getForEntityAndAttribute(
        id: URI,
        attributeName: String,
        datasetId: URI? = null
    ): Either<APIException, TemporalEntityAttribute> {
        val selectQuery =
            """
            SELECT *
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            AND attribute_name = :attribute_name
            ${datasetId.toDatasetIdFilter()}
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", id)
            .bind("attribute_name", attributeName)
            .let {
                if (datasetId != null) it.bind("dataset_id", datasetId)
                else it
            }
            .oneToResult {
                rowToTemporalEntityAttribute(it)
            }
    }

    suspend fun hasAttribute(
        id: URI,
        attributeName: String,
        datasetId: URI? = null
    ): Either<APIException, Boolean> {
        val selectQuery =
            """
            SELECT count(entity_id) as count
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            ${datasetId.toDatasetIdFilter()}
            AND attribute_name = :attribute_name
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", id)
            .bind("attribute_name", attributeName)
            .let {
                if (datasetId != null) it.bind("dataset_id", datasetId)
                else it
            }
            .oneToResult {
                it["count"] as Long == 1L
            }
    }

    private fun rowToTemporalEntityAttribute(row: Map<String, Any>) =
        TemporalEntityAttribute(
            id = toUuid(row["id"]),
            entityId = toUri(row["entity_id"]),
            attributeName = row["attribute_name"] as ExpandedTerm,
            attributeType = TemporalEntityAttribute.AttributeType.valueOf(row["attribute_type"] as String),
            attributeValueType = TemporalEntityAttribute.AttributeValueType.valueOf(
                row["attribute_value_type"] as String
            ),
            datasetId = toOptionalUri(row["dataset_id"]),
            createdAt = toZonedDateTime(row["created_at"]),
            modifiedAt = toOptionalZonedDateTime(row["modified_at"]),
            payload = toJsonString(row["payload"])
        )

    suspend fun checkEntityAndAttributeExistence(
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI? = null
    ): Either<APIException, Unit> {
        val selectQuery =
            """
                select 
                    exists(
                        select 1 
                        from temporal_entity_attribute 
                        where entity_id = :entity_id
                    ) as entityExists,
                    exists(
                        select 1 
                        from temporal_entity_attribute 
                        where entity_id = :entity_id 
                        and attribute_name = :attribute_name
                        ${datasetId.toDatasetIdFilter()}
                    ) as attributeNameExists;
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", entityId)
            .bind("attribute_name", attributeName)
            .let {
                if (datasetId != null) it.bind("dataset_id", datasetId)
                else it
            }
            .oneToResult { Pair(it["entityExists"] as Boolean, it["attributeNameExists"] as Boolean) }
            .flatMap {
                if (it.first) {
                    if (it.second)
                        Unit.right()
                    else ResourceNotFoundException(attributeNotFoundMessage(attributeName, datasetId)).left()
                } else ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()
            }
    }

    @Transactional
    suspend fun appendEntityAttributes(
        entityUri: URI,
        ngsiLdAttributes: List<NgsiLdAttribute>,
        jsonLdAttributes: Map<String, Any>,
        disallowOverwrite: Boolean,
        sub: Sub?
    ): Either<APIException, UpdateResult> =
        either {
            val attributeInstances = ngsiLdAttributes.flatOnInstances()
            val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
            attributeInstances.parTraverseEither { (ngsiLdAttribute, ngsiLdAttributeInstance) ->
                logger.debug("Appending attribute ${ngsiLdAttribute.name} in entity $entityUri")
                val currentTea =
                    getForEntityAndAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId)
                        .fold({ null }, { it })
                val attributeMetadata = ngsiLdAttributeInstance.toTemporalAttributeMetadata().bind()
                val attributePayload = getAttributeFromExpandedAttributes(
                    jsonLdAttributes,
                    ngsiLdAttribute.name,
                    ngsiLdAttributeInstance.datasetId
                )!!
                if (currentTea == null) {
                    addAttribute(
                        entityUri,
                        ngsiLdAttribute,
                        attributeMetadata,
                        createdAt,
                        attributePayload,
                        sub
                    ).bind()
                    UpdateAttributeResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        UpdateOperationResult.APPENDED,
                        null
                    ).right()
                } else if (disallowOverwrite) {
                    val message = "Attribute already exists on $entityUri and overwrite is not allowed, ignoring"
                    logger.info(message)
                    UpdateAttributeResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        UpdateOperationResult.IGNORED,
                        message
                    ).right()
                } else {
                    replaceAttribute(
                        currentTea,
                        ngsiLdAttribute,
                        attributeMetadata,
                        createdAt,
                        attributePayload,
                        sub
                    ).bind()
                    UpdateAttributeResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        UpdateOperationResult.REPLACED,
                        null
                    ).right()
                }
            }.tap {
                // update modifiedAt in entity if at least one attribute has been added
                if (it.hasSuccessfulUpdate()) {
                    val teas = getForEntity(entityUri, emptySet())
                    entityPayloadService.updateState(entityUri, createdAt, teas).bind()
                }
            }.fold({
                it
            }, {
                updateResultFromDetailedResult(it)
            })
        }

    @Transactional
    suspend fun updateEntityAttributes(
        entityUri: URI,
        ngsiLdAttributes: List<NgsiLdAttribute>,
        jsonLdAttributes: Map<String, Any>,
        sub: Sub?
    ): Either<APIException, UpdateResult> =
        either {
            val attributeInstances = ngsiLdAttributes.flatOnInstances()
            val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
            attributeInstances.parTraverseEither { (ngsiLdAttribute, ngsiLdAttributeInstance) ->
                logger.debug("Updating attribute ${ngsiLdAttribute.name} in entity $entityUri")
                val currentTea =
                    getForEntityAndAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId)
                        .fold({ null }, { it })
                if (currentTea != null) {
                    val attributeMetadata = ngsiLdAttributeInstance.toTemporalAttributeMetadata().bind()
                    val attributePayload = getAttributeFromExpandedAttributes(
                        jsonLdAttributes,
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId
                    )!!
                    replaceAttribute(
                        currentTea,
                        ngsiLdAttribute,
                        attributeMetadata,
                        createdAt,
                        attributePayload,
                        sub
                    ).bind()
                    UpdateAttributeResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        UpdateOperationResult.REPLACED,
                        null
                    ).right()
                } else {
                    val message = if (ngsiLdAttributeInstance.datasetId != null)
                        "Attribute (datasetId: ${ngsiLdAttributeInstance.datasetId}) does not exist"
                    else
                        "Attribute (default instance) does not exist"
                    logger.info(message)
                    UpdateAttributeResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        UpdateOperationResult.IGNORED,
                        message
                    ).right()
                }
            }.tap {
                // update modifiedAt in entity if at least one attribute has been added
                if (it.hasSuccessfulUpdate()) {
                    val teas = getForEntity(entityUri, emptySet())
                    entityPayloadService.updateState(entityUri, createdAt, teas).bind()
                }
            }.fold({
                it
            }, {
                updateResultFromDetailedResult(it)
            })
        }

    @Transactional
    suspend fun partialUpdateEntityAttribute(
        entityId: URI,
        expandedPayload: Map<String, List<Map<String, List<Any>>>>,
        sub: Sub?
    ): Either<APIException, UpdateResult> =
        either {
            val expandedAttributeName = expandedPayload.keys.first()
            val attributeValues = expandedPayload.values.first()[0]
            val modifiedAt = ZonedDateTime.now(ZoneOffset.UTC)
            logger.debug("Updating attribute $expandedAttributeName of entity $entityId with values: $attributeValues")

            val datasetId = attributeValues.getDatasetId()
            val exists = hasAttribute(entityId, expandedAttributeName, datasetId).bind()
            val updateAttributeResult =
                if (exists) {
                    // first update payload in temporal entity attribute
                    val tea = getForEntityAndAttribute(entityId, expandedAttributeName, datasetId).bind()
                    val jsonSourceObject = JSONObject(tea.payload)
                    val jsonUpdateObject = JSONObject(attributeValues)
                    val jsonMerger = JsonMerger(
                        arrayMergeMode = JsonMerger.ArrayMergeMode.REPLACE_ARRAY,
                        objectMergeMode = JsonMerger.ObjectMergeMode.MERGE_OBJECT
                    )
                    val jsonTargetObject = jsonMerger.merge(jsonSourceObject, jsonUpdateObject)
                    val deserializedPayload = jsonTargetObject.toMap() as ExpandedAttributePayloadEntry
                    updateStatus(tea.id, modifiedAt, jsonTargetObject.toString()).bind()

                    // then update attribute instance
                    val isNewObservation = attributeValues.containsKey(NGSILD_OBSERVED_AT_PROPERTY)
                    val timeAndProperty =
                        if (isNewObservation)
                            Pair(
                                getPropertyValueFromMap(
                                    attributeValues, NGSILD_OBSERVED_AT_PROPERTY
                                )!! as ZonedDateTime,
                                AttributeInstance.TemporalProperty.OBSERVED_AT
                            )
                        else
                            Pair(modifiedAt, AttributeInstance.TemporalProperty.MODIFIED_AT)

                    val value = getValueFromPartialAttributePayload(tea, deserializedPayload)
                    val attributeInstance = AttributeInstance(
                        temporalEntityAttribute = tea.id,
                        timeProperty = timeAndProperty.second,
                        time = timeAndProperty.first,
                        value = value.first,
                        measuredValue = value.second,
                        geoValue = value.third,
                        payload = deserializedPayload,
                        sub = sub
                    )
                    attributeInstanceService.create(attributeInstance).bind()

                    UpdateAttributeResult(
                        expandedAttributeName,
                        datasetId,
                        UpdateOperationResult.UPDATED,
                        null
                    )
                } else {
                    UpdateAttributeResult(
                        expandedAttributeName,
                        datasetId,
                        UpdateOperationResult.IGNORED,
                        "Unknown attribute $expandedAttributeName with datasetId $datasetId in entity $entityId"
                    )
                }

            if (updateAttributeResult.isSuccessfullyUpdated()) {
                val teas = getForEntity(entityId, emptySet())
                entityPayloadService.updateState(entityId, modifiedAt, teas).bind()
            }

            updateResultFromDetailedResult(listOf(updateAttributeResult))
        }

    private fun getValueFromPartialAttributePayload(
        tea: TemporalEntityAttribute,
        attributePayload: Map<String, List<Any>>
    ): Triple<String?, Double?, WKTCoordinates?> =
        when (tea.attributeType) {
            TemporalEntityAttribute.AttributeType.Property ->
                Triple(
                    valueToStringOrNull(getPropertyValueFromMap(attributePayload, NGSILD_PROPERTY_VALUE)!!),
                    valueToDoubleOrNull(getPropertyValueFromMap(attributePayload, NGSILD_PROPERTY_VALUE)!!),
                    null
                )
            TemporalEntityAttribute.AttributeType.Relationship ->
                Triple(
                    getPropertyValueFromMap(attributePayload, NGSILD_RELATIONSHIP_HAS_OBJECT)!! as String,
                    null,
                    null
                )
            TemporalEntityAttribute.AttributeType.GeoProperty ->
                Triple(
                    null,
                    null,
                    WKTCoordinates(getPropertyValueFromMap(attributePayload, NGSILD_PROPERTY_VALUE)!! as String)
                )
        }
}
