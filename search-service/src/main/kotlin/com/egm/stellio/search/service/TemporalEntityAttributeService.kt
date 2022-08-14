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
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.getAttributeFromExpandedAttributes
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.JsonUtils.deserializeExpandedPayload
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.attributeNotFoundMessage
import com.egm.stellio.shared.util.entityNotFoundMessage
import com.egm.stellio.shared.util.mapper
import com.fasterxml.jackson.databind.JsonNode
import io.r2dbc.postgresql.codec.Json
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
        return createEntityTemporalReferences(
            jsonLdEntity.toNgsiLdEntity(),
            jsonLdEntity,
            sub
        )
    }

    @Transactional
    suspend fun createEntityTemporalReferences(
        ngsiLdEntity: NgsiLdEntity,
        jsonLdEntity: JsonLdEntity,
        sub: String? = null
    ): Either<APIException, Unit> {
        logger.debug("Creating entity ${ngsiLdEntity.id}")

        val temporalAttributes = prepareTemporalAttributes(ngsiLdEntity).ifEmpty { return Unit.right() }
        val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
        logger.debug("Found ${temporalAttributes.size} supported attributes in entity: ${ngsiLdEntity.id}")

        return temporalAttributes.parTraverseEither {
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
        entityId: URI,
        modifiedAt: ZonedDateTime,
        payload: Map<String, Any>
    ): Either<APIException, Unit> =
        updateStatus(entityId, modifiedAt, serializeObject(payload))

    @Transactional
    suspend fun updateStatus(
        entityId: URI,
        modifiedAt: ZonedDateTime,
        payload: String
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET payload = :payload,
                modified_at = :modified_at
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("payload", Json.of(payload))
            .bind("modified_at", modifiedAt)
            .execute()

    suspend fun updateSpecificAccessPolicy(
        entityId: URI,
        specificAccessPolicy: SpecificAccessPolicy
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET specific_access_policy = :specific_access_policy
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("specific_access_policy", specificAccessPolicy.toString())
            .execute()

    suspend fun removeSpecificAccessPolicy(entityId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET specific_access_policy = null
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()

    suspend fun hasSpecificAccessPolicies(
        entityId: URI,
        specificAccessPolicies: List<SpecificAccessPolicy>
    ): Either<APIException, Boolean> =
        databaseClient.sql(
            """
            SELECT count(id) as count
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            AND specific_access_policy IN (:specific_access_policies)
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("specific_access_policies", specificAccessPolicies.map { it.toString() })
            .oneToResult { it["count"] as Long > 0 }

    suspend fun addAttribute(
        entityId: URI,
        ngsiLdAttribute: NgsiLdAttribute,
        attributeMetadata: AttributeMetadata,
        createdAt: ZonedDateTime,
        attributePayload: Map<String, Any>,
        sub: Sub?
    ): Either<APIException, Unit> =
        either {
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

    private fun prepareTemporalAttributes(ngsiLdEntity: NgsiLdEntity): List<Pair<String, AttributeMetadata>> =
        ngsiLdEntity.attributes
            .flatMapTo(
                arrayListOf()
            ) {
                it.getAttributeInstances().map { instance ->
                    Pair(it.name, toTemporalAttributeMetadata(instance))
                }
            }.filter {
                it.second.isRight()
            }.map {
                Pair(it.first, it.second.orNull()!!)
            }

    internal fun toTemporalAttributeMetadata(
        ngsiLdAttributeInstance: NgsiLdAttributeInstance
    ): Either<APIException, AttributeMetadata> {
        val attributeType = when (ngsiLdAttributeInstance) {
            is NgsiLdPropertyInstance -> TemporalEntityAttribute.AttributeType.Property
            is NgsiLdRelationshipInstance -> TemporalEntityAttribute.AttributeType.Relationship
            is NgsiLdGeoPropertyInstance -> TemporalEntityAttribute.AttributeType.GeoProperty
        }
        val attributeValue = when (ngsiLdAttributeInstance) {
            is NgsiLdPropertyInstance ->
                Triple(
                    valueToStringOrNull(ngsiLdAttributeInstance.value),
                    valueToDoubleOrNull(ngsiLdAttributeInstance.value),
                    null
                )
            is NgsiLdRelationshipInstance -> Triple(ngsiLdAttributeInstance.objectId.toString(), null, null)
            is NgsiLdGeoPropertyInstance -> Triple(null, null, ngsiLdAttributeInstance.coordinates)
            else -> Triple(null, null, null)
        }
        if (attributeValue == Triple(null, null, null)) {
            return BadRequestDataException("Unable to get a value from attribute: $ngsiLdAttributeInstance").left()
        }
        val attributeValueType =
            if (attributeValue.second != null) TemporalEntityAttribute.AttributeValueType.MEASURE
            else if (attributeValue.third != null) TemporalEntityAttribute.AttributeValueType.GEOMETRY
            else TemporalEntityAttribute.AttributeValueType.ANY

        return AttributeMetadata(
            measuredValue = attributeValue.second,
            value = attributeValue.first,
            geoValue = attributeValue.third,
            valueType = attributeValueType,
            datasetId = ngsiLdAttributeInstance.datasetId,
            type = attributeType,
            observedAt = ngsiLdAttributeInstance.observedAt
        ).right()
    }

    suspend fun getForEntities(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): List<TemporalEntityAttribute> {
        val filterQuery = buildEntitiesQueryFilter(
            queryParams,
            accessRightFilter
        )

        val filterOnAttributesQuery = buildEntitiesQueryFilter(
            queryParams.copy(ids = emptySet(), idPattern = null, types = emptySet()),
            accessRightFilter,
            " AND "
        )

        val selectQuery =
            """
                WITH entities AS (
                    SELECT DISTINCT(temporal_entity_attribute.entity_id)
                    FROM temporal_entity_attribute
                    JOIN entity_payload ON temporal_entity_attribute.entity_id = entity_payload.entity_id
                    WHERE $filterQuery
                    ORDER BY entity_id
                    LIMIT :limit
                    OFFSET :offset   
                )
                SELECT id, entity_id, attribute_name, attribute_type, attribute_value_type, created_at, modified_at,
                    dataset_id, payload
                FROM temporal_entity_attribute            
                WHERE entity_id IN (SELECT entity_id FROM entities) $filterOnAttributesQuery
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
        val selectStatement =
            """
            SELECT count(distinct(temporal_entity_attribute.entity_id)) as count_entity
            FROM temporal_entity_attribute
            JOIN entity_payload ON temporal_entity_attribute.entity_id = entity_payload.entity_id
            WHERE
            """.trimIndent()

        val filterQuery = buildEntitiesQueryFilter(
            queryParams,
            accessRightFilter
        )
        return databaseClient
            .sql("$selectStatement $filterQuery")
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
                    prefix = "temporal_entity_attribute.entity_id in(",
                    postfix = ")"
                ) { "'$it'" }
            else null
        val formattedIdPattern =
            if (!queryParams.idPattern.isNullOrEmpty())
                "temporal_entity_attribute.entity_id ~ '${queryParams.idPattern}'"
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
        entityAttributeName: String
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
                    ) as attributeNameExists;
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", entityId)
            .bind("attribute_name", entityAttributeName)
            .oneToResult { Pair(it["entityExists"] as Boolean, it["attributeNameExists"] as Boolean) }
            .flatMap {
                if (it.first) {
                    if (it.second)
                        Unit.right()
                    else ResourceNotFoundException(attributeNotFoundMessage(entityAttributeName)).left()
                } else ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()
            }
    }

    suspend fun checkEntityExistence(
        entityId: URI
    ): Either<APIException, Unit> {
        val selectQuery =
            """
                select 
                    exists(
                        select 1 
                        from temporal_entity_attribute 
                        where entity_id = :entity_id
                    ) as entityExists;
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", entityId)
            .oneToResult { it["entityExists"] as Boolean }
            .flatMap {
                if (it) Unit.right()
                else ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()
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
                logger.debug("Gonna append attribute ${ngsiLdAttribute.name} in entity $entityUri")
                val exists = hasAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId).bind()
                val attributeMetadata = toTemporalAttributeMetadata(ngsiLdAttributeInstance).bind()
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
                    val message = "Attribute ${ngsiLdAttribute.name} already exists on $entityUri " +
                        "and overwrite is not allowed, ignoring"
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
                logger.debug("Gonna update attribute ${ngsiLdAttribute.name} in entity $entityUri")
                val exists = hasAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId).bind()
                val attributeMetadata = toTemporalAttributeMetadata(ngsiLdAttributeInstance).bind()
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
                        "Attribute ${ngsiLdAttribute.name} " +
                            "(datasetId: ${ngsiLdAttributeInstance.datasetId}) does not exist"
                    else
                        "Attribute ${ngsiLdAttribute.name} (default instance) does not exist"
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
                    val currentNode = mapper.readValue(tea.payload, JsonNode::class.java)
                    val updatedNode = mapper.readerForUpdating(currentNode)
                        .readValue(serializeObject(attributeInstanceValues), JsonNode::class.java)
                    val payload = mapper.writeValueAsString(updatedNode)
                    val deserializedPayload = payload.deserializeExpandedPayload()
                    updateStatus(entityId, modifiedAt, payload).bind()

                    // then update attribute instance
                    val isNewObservation = attributeInstanceValues.containsKey(NGSILD_OBSERVED_AT_PROPERTY)
                    val timeAndProperty =
                        if (isNewObservation)
                            Pair(
                                ZonedDateTime.parse(
                                    getPropertyValueFromMap(
                                        attributeInstanceValues, NGSILD_OBSERVED_AT_PROPERTY
                                    )!! as String
                                ),
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
