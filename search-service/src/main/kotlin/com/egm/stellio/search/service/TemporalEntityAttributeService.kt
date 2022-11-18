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
    ): Either<APIException, Unit> {
        logger.debug("Creating entity ${ngsiLdEntity.id}")

        val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
        logger.debug("Creating ${attributesMetadata.size} attributes in entity: ${ngsiLdEntity.id}")

        return attributesMetadata.parTraverseEither {
            either<APIException, Unit> {
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
        }.map {
            entityPayloadService.createEntityPayload(ngsiLdEntity.id, ngsiLdEntity.types, createdAt, jsonLdEntity)
        }
    }

    @Transactional
    suspend fun updateStatus(
        teaUUID: UUID,
        modifiedAt: ZonedDateTime,
        payload: Map<String, Any>
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
        attributePayload: Map<String, Any>,
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

    suspend fun deleteTemporalEntityReferences(entityId: URI): Either<APIException, Unit> =
        either {
            logger.debug("Deleting entity $entityId")
            entityPayloadService.deleteEntityPayload(entityId).bind()
            deleteTemporalAttributesOfEntity(entityId).bind()
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
            if (deleteAll)
                deleteTemporalAttributeAllInstancesReferences(entityId, attributeName).bind()
            else
                deleteTemporalAttributeReferences(entityId, attributeName, datasetId).bind()
            entityPayloadService.updateLastModificationDate(entityId, ZonedDateTime.now(ZoneOffset.UTC)).bind()
        }

    suspend fun deleteTemporalAttributeReferences(
        entityId: URI,
        attributeName: String,
        datasetId: URI?
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM temporal_entity_attribute WHERE 
                entity_id = :entity_id
                ${if (datasetId != null) "AND dataset_id = :dataset_id" else "AND dataset_id IS NULL"}
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
    ): List<TemporalEntityAttribute> {
        val filterQuery = buildEntitiesQueryFilter(
            queryParams,
            accessRightFilter
        ).let {
            if (queryParams.q != null)
                it.plus(" AND (").plus(buildInnerQuery(queryParams.q!!, queryParams.context)).plus(")")
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
                    SELECT DISTINCT(tea1.entity_id)
                    FROM temporal_entity_attribute tea1
                    JOIN entity_payload ON tea1.entity_id = entity_payload.entity_id
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
            SELECT count(distinct(tea1.entity_id)) as count_entity
            FROM temporal_entity_attribute tea1
            JOIN entity_payload ON tea1.entity_id = entity_payload.entity_id
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
                    prefix = "tea1.entity_id in(",
                    postfix = ")"
                ) { "'$it'" }
            else null
        val formattedIdPattern =
            if (!queryParams.idPattern.isNullOrEmpty())
                "tea1.entity_id ~ '${queryParams.idPattern}'"
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
            val targetValue = query.third.convertInDateTimeIfNeeded(query.second)
            """
            EXISTS(
               SELECT 1
               FROM temporal_entity_attribute
               WHERE tea1.entity_id = temporal_entity_attribute.entity_id
               AND (attribute_name = '${expandJsonLdTerm(query.first, listOf(context))}'
                    AND CASE 
                        WHEN attribute_type = 'Property' THEN 
                            jsonb_path_exists(temporal_entity_attribute.payload,
                                '$."$NGSILD_PROPERTY_VALUE" ? 
                                    (@."$JSONLD_VALUE_KW" ${query.second} $targetValue)')
                        WHEN attribute_type = 'Property' 
                            AND attribute_value_type IN ('DATETIME', 'DATE', 'TIME')
                            AND '${query.second}' != 'like_regex' THEN
                                jsonb_path_exists(temporal_entity_attribute.payload,
                                    '$."$NGSILD_PROPERTY_VALUE" ? 
                                        (@."$JSONLD_VALUE_KW".datetime() ${query.second} $targetValue)')
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

    /**
     * Parse a query term to return a triple consisting of (attribute, operator, comparable value)
     */
    private fun extractComparisonParametersFromQuery(queryTerm: String): Triple<String, String, String> {
        return when {
            queryTerm.contains("==") ->
                Triple(queryTerm.split("==")[0], "==", queryTerm.split("==")[1])
            queryTerm.contains("!=") ->
                Triple(queryTerm.split("!=")[0], "<>", queryTerm.split("!=")[1])
            queryTerm.contains(">=") ->
                Triple(queryTerm.split(">=")[0], ">=", queryTerm.split(">=")[1])
            queryTerm.contains(">") ->
                Triple(queryTerm.split(">")[0], ">", queryTerm.split(">")[1])
            queryTerm.contains("<=") ->
                Triple(queryTerm.split("<=")[0], "<=", queryTerm.split("<=")[1])
            queryTerm.contains("<") ->
                Triple(queryTerm.split("<")[0], "<", queryTerm.split("<")[1])
            queryTerm.contains("=~") ->
                Triple(queryTerm.split("=~")[0], "like_regex", queryTerm.split("=~")[1])
            else -> throw OperationNotSupportedException("Unsupported query term : $queryTerm")
        }
    }

    private fun String.convertInDateTimeIfNeeded(regexPattern: String) =
        if (this.isDate() || this.isDateTime() || this.isTime())
            if (regexPattern != "like_regex")
                "\"".plus(this).plus("\"").plus(".datetime()")
            else "\"".plus(this).plus("\"")
        else
            this

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
            ${if (datasetId != null) "AND dataset_id = :dataset_id" else "AND dataset_id IS NULL"}
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
            ${if (datasetId != null) "AND dataset_id = :dataset_id" else "AND dataset_id IS NULL"}
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
        entityAttributeName: String,
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
                        ${if (datasetId != null) "AND dataset_id = :dataset_id" else "AND dataset_id IS NULL"}
                    ) as attributeNameExists;
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", entityId)
            .bind("attribute_name", entityAttributeName)
            .let {
                if (datasetId != null) it.bind("dataset_id", datasetId)
                else it
            }
            .oneToResult { Pair(it["entityExists"] as Boolean, it["attributeNameExists"] as Boolean) }
            .flatMap {
                if (it.first) {
                    if (it.second)
                        Unit.right()
                    else ResourceNotFoundException(attributeNotFoundMessage(entityAttributeName)).left()
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
            val attributeInstances = ngsiLdAttributes
                .flatMap { ngsiLdAttribute ->
                    ngsiLdAttribute.getAttributeInstances().map { Pair(ngsiLdAttribute, it) }
                }
            val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
            attributeInstances.parTraverseEither { (ngsiLdAttribute, ngsiLdAttributeInstance) ->
                logger.debug("Appending attribute ${ngsiLdAttribute.name} in entity $entityUri")
                val exists = hasAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId).bind()
                val attributeMetadata = ngsiLdAttributeInstance.toTemporalAttributeMetadata().bind()
                val attributePayload = getAttributeFromExpandedAttributes(
                    jsonLdAttributes,
                    ngsiLdAttribute.name,
                    ngsiLdAttributeInstance.datasetId
                )!!
                if (!exists) {
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
                    deleteTemporalAttributeReferences(
                        entityUri,
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId
                    ).bind()
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
                        UpdateOperationResult.REPLACED,
                        null
                    ).right()
                }
            }.tap {
                // update modifiedAt in entity if at least one attribute has been added
                if (it.isNotEmpty())
                    entityPayloadService.updateLastModificationDate(entityUri, createdAt).bind()
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
            val attributeInstances = ngsiLdAttributes
                .flatMap { ngsiLdAttribute ->
                    ngsiLdAttribute.getAttributeInstances().map { Pair(ngsiLdAttribute, it) }
                }
            val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
            attributeInstances.parTraverseEither { (ngsiLdAttribute, ngsiLdAttributeInstance) ->
                logger.debug("Updating attribute ${ngsiLdAttribute.name} in entity $entityUri")
                val exists = hasAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId).bind()
                val attributeMetadata = ngsiLdAttributeInstance.toTemporalAttributeMetadata().bind()
                val attributePayload = getAttributeFromExpandedAttributes(
                    jsonLdAttributes,
                    ngsiLdAttribute.name,
                    ngsiLdAttributeInstance.datasetId
                )!!
                if (exists) {
                    deleteTemporalAttributeReferences(
                        entityUri,
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId
                    ).bind()
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
                if (it.isNotEmpty())
                    entityPayloadService.updateLastModificationDate(entityUri, createdAt).bind()
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
            val attributeValues = expandedPayload.values.first()
            val modifiedAt = ZonedDateTime.now(ZoneOffset.UTC)
            logger.debug("Updating attribute $expandedAttributeName of entity $entityId with values: $attributeValues")

            attributeValues.parTraverseEither { attributeInstanceValues ->
                val datasetId = attributeInstanceValues.getDatasetId()
                val exists = hasAttribute(entityId, expandedAttributeName, datasetId).bind()
                if (exists) {
                    // first update payload in temporal entity attribute
                    val tea = getForEntityAndAttribute(entityId, expandedAttributeName, datasetId).bind()
                    val jsonSourceObject = JSONObject(tea.payload)
                    val jsonUpdateObject = JSONObject(attributeInstanceValues)
                    val jsonMerger = JsonMerger(
                        arrayMergeMode = JsonMerger.ArrayMergeMode.REPLACE_ARRAY,
                        objectMergeMode = JsonMerger.ObjectMergeMode.MERGE_OBJECT
                    )
                    val jsonTargetObject = jsonMerger.merge(jsonSourceObject, jsonUpdateObject)
                    val deserializedPayload = jsonTargetObject.toMap() as Map<String, List<Any>>
                    updateStatus(tea.id, modifiedAt, jsonTargetObject.toString()).bind()

                    // then update attribute instance
                    val isNewObservation = attributeInstanceValues.containsKey(NGSILD_OBSERVED_AT_PROPERTY)
                    val timeAndProperty =
                        if (isNewObservation)
                            Pair(
                                getPropertyValueFromMap(
                                    attributeInstanceValues, NGSILD_OBSERVED_AT_PROPERTY
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
                    ).right()
                } else {
                    UpdateAttributeResult(
                        expandedAttributeName,
                        datasetId,
                        UpdateOperationResult.IGNORED,
                        "Unknown attribute $expandedAttributeName with datasetId $datasetId in entity $entityId"
                    ).right()
                }
            }.tap { updateAttributeResults ->
                // update modifiedAt in entity if at least one attribute has been added
                if (updateAttributeResults.any { it.isSuccessfullyUpdated() })
                    entityPayloadService.updateLastModificationDate(entityId, modifiedAt).bind()
            }.fold({
                it
            }, {
                updateResultFromDetailedResult(it)
            })
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
