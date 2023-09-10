package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import arrow.fx.coroutines.parMap
import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedDateTime
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.getAttributeFromExpandedAttributes
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.savvasdalkitsis.jsonmerger.JsonMerger
import io.r2dbc.postgresql.codec.Json
import org.json.JSONObject
import org.slf4j.LoggerFactory
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

@Service
class TemporalEntityAttributeService(
    private val databaseClient: DatabaseClient,
    private val attributeInstanceService: AttributeInstanceService
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
            .bind("payload", temporalEntityAttribute.payload)
            .execute()

    @Transactional
    suspend fun updateOnReplace(
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
    suspend fun updateOnUpdate(
        teaUUID: UUID,
        valueType: TemporalEntityAttribute.AttributeValueType,
        modifiedAt: ZonedDateTime,
        payload: String
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET payload = :payload,
                attribute_value_type = :attribute_value_type,
                modified_at = :modified_at
            WHERE id = :tea_uuid
            """.trimIndent()
        )
            .bind("tea_uuid", teaUUID)
            .bind("payload", Json.of(payload))
            .bind("attribute_value_type", valueType.toString())
            .bind("modified_at", modifiedAt)
            .execute()

    /**
     * Currently only used in the tests for easy creation of attributes.
     *
     * To be removed at some point later.
     */
    @Transactional
    suspend fun createEntityTemporalReferences(
        payload: String,
        contexts: List<String>,
        sub: String? = null
    ): Either<APIException, Unit> = either {
        val createdAt = ZonedDateTime.now(ZoneOffset.UTC)
        val jsonLdEntity = expandJsonLdEntity(payload, contexts)
        val ngsiLdEntity = jsonLdEntity.toNgsiLdEntity().bind()
        ngsiLdEntity.prepareTemporalAttributes()
            .map {
                createEntityTemporalReferences(ngsiLdEntity, jsonLdEntity, it, createdAt, sub).bind()
            }.bind()
    }

    @Transactional
    suspend fun createEntityTemporalReferences(
        ngsiLdEntity: NgsiLdEntity,
        jsonLdEntity: JsonLdEntity,
        attributesMetadata: List<Pair<ExpandedTerm, AttributeMetadata>>,
        createdAt: ZonedDateTime,
        sub: String? = null
    ): Either<APIException, Unit> = either {
        logger.debug("Creating {} attributes in entity: {}", attributesMetadata.size, ngsiLdEntity.id)

        attributesMetadata
            .filter {
                it.first != AuthContextModel.AUTH_PROP_SAP
            }
            .forEach {
                val (expandedAttributeName, attributeMetadata) = it
                val attributePayload = getAttributeFromExpandedAttributes(
                    jsonLdEntity.members,
                    expandedAttributeName,
                    attributeMetadata.datasetId
                )!!

                addAttribute(
                    ngsiLdEntity.id,
                    expandedAttributeName,
                    attributeMetadata,
                    createdAt,
                    attributePayload,
                    sub
                ).bind()
            }
    }

    @Transactional
    suspend fun addAttribute(
        entityId: URI,
        attributeName: ExpandedTerm,
        attributeMetadata: AttributeMetadata,
        createdAt: ZonedDateTime,
        attributePayload: ExpandedAttributeInstance,
        sub: Sub?
    ): Either<APIException, Unit> =
        either {
            logger.debug("Adding attribute {} to entity {}", attributeName, entityId)
            val temporalEntityAttribute = TemporalEntityAttribute(
                entityId = entityId,
                attributeName = attributeName,
                attributeType = attributeMetadata.type,
                attributeValueType = attributeMetadata.valueType,
                datasetId = attributeMetadata.datasetId,
                createdAt = createdAt,
                payload = Json.of(serializeObject(attributePayload))
            )
            create(temporalEntityAttribute).bind()

            val attributeInstance = AttributeInstance(
                temporalEntityAttribute = temporalEntityAttribute.id,
                timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
                time = createdAt,
                attributeMetadata = attributeMetadata,
                payload = attributePayload,
                sub = sub
            )
            attributeInstanceService.create(attributeInstance).bind()

            if (attributeMetadata.observedAt != null) {
                val attributeObservedAtInstance = AttributeInstance(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    time = attributeMetadata.observedAt,
                    attributeMetadata = attributeMetadata,
                    payload = attributePayload
                )
                attributeInstanceService.create(attributeObservedAtInstance).bind()
            }
        }

    @Transactional
    suspend fun replaceAttribute(
        temporalEntityAttribute: TemporalEntityAttribute,
        ngsiLdAttribute: NgsiLdAttribute,
        attributeMetadata: AttributeMetadata,
        createdAt: ZonedDateTime,
        attributePayload: ExpandedAttributeInstance,
        sub: Sub?
    ): Either<APIException, Unit> =
        either {
            logger.debug(
                "Replacing attribute {} ({}) in entity {}",
                ngsiLdAttribute.name,
                attributeMetadata.datasetId,
                temporalEntityAttribute.entityId
            )
            updateOnReplace(
                temporalEntityAttribute.id,
                attributeMetadata,
                createdAt,
                serializeObject(attributePayload)
            ).bind()

            val attributeInstance = AttributeInstance(
                temporalEntityAttribute = temporalEntityAttribute.id,
                timeProperty = AttributeInstance.TemporalProperty.MODIFIED_AT,
                time = createdAt,
                attributeMetadata = attributeMetadata,
                payload = attributePayload,
                sub = sub
            )
            attributeInstanceService.create(attributeInstance).bind()

            if (attributeMetadata.observedAt != null) {
                val attributeObservedAtInstance = AttributeInstance(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    time = attributeMetadata.observedAt,
                    attributeMetadata = attributeMetadata,
                    payload = attributePayload
                )
                attributeInstanceService.create(attributeObservedAtInstance).bind()
            }
        }

    @Transactional
    suspend fun mergeAttribute(
        tea: TemporalEntityAttribute,
        attributeName: ExpandedTerm,
        attributeMetadata: AttributeMetadata,
        mergedAt: ZonedDateTime,
        observedAt: ZonedDateTime?,
        attributePayload: ExpandedAttributeInstance,
        sub: Sub?
    ): Either<APIException, Unit> = either {
        logger.debug(
            "Merging attribute {} ({}) in entity {}",
            attributeName,
            attributeMetadata.datasetId,
            tea.entityId
        )
        val (processedAttributePayload, processedAttributeMetadata) = processObservedAtInMergeOperation(
            tea,
            attributePayload,
            attributeMetadata,
            observedAt
        )
        val (jsonTargetObject, updatedAttributeInstance) = mergeAttributePayload(tea, processedAttributePayload)
        val value = getValueFromPartialAttributePayload(tea, updatedAttributeInstance)
        updateOnUpdate(tea.id, processedAttributeMetadata.valueType, mergedAt, jsonTargetObject.toString()).bind()

        val attributeInstance =
            createContextualAttributeInstance(tea, updatedAttributeInstance, value, mergedAt, sub)
        attributeInstanceService.create(attributeInstance).bind()
    }

    @Transactional
    suspend fun deleteTemporalAttributesOfEntity(entityId: URI): Either<APIException, Unit> {
        val uuids = databaseClient.sql(
            """
            DELETE FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            RETURNING id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .allToMappedList {
                toUuid(it["id"])
            }

        return if (uuids.isNotEmpty())
            attributeInstanceService.deleteInstancesOfEntity(uuids)
        else Unit.right()
    }

    @Transactional
    suspend fun deleteTemporalAttribute(
        entityId: URI,
        attributeName: String,
        datasetId: URI?,
        deleteAll: Boolean = false
    ): Either<APIException, Unit> =
        either {
            logger.debug("Deleting attribute {} from entity {} (all: {})", attributeName, entityId, deleteAll)
            if (deleteAll) {
                attributeInstanceService.deleteAllInstancesOfAttribute(entityId, attributeName).bind()
                deleteTemporalAttributeAllInstancesReferences(entityId, attributeName).bind()
            } else {
                attributeInstanceService.deleteInstancesOfAttribute(entityId, attributeName, datasetId).bind()
                deleteTemporalAttributeReferences(entityId, attributeName, datasetId).bind()
            }
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

    suspend fun getForTemporalEntities(
        entitiesIds: List<URI>,
        queryParams: QueryParams
    ): List<TemporalEntityAttribute> {
        val filterOnAttributes =
            if (queryParams.attrs.isNotEmpty())
                " AND " + queryParams.attrs.joinToString(
                    separator = ",",
                    prefix = "attribute_name in (",
                    postfix = ")"
                ) { "'$it'" }
            else ""

        val selectQuery =
            """
            SELECT id, entity_id, attribute_name, attribute_type, attribute_value_type, created_at, modified_at,
                dataset_id, payload
            FROM temporal_entity_attribute            
            WHERE entity_id IN (:entities_ids) 
            $filterOnAttributes
            ORDER BY entity_id
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entities_ids", entitiesIds)
            .allToMappedList { rowToTemporalEntityAttribute(it) }
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
            payload = toJson(row["payload"])
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
        expandedAttributes: ExpandedAttributes,
        disallowOverwrite: Boolean,
        createdAt: ZonedDateTime,
        sub: Sub?
    ): Either<APIException, UpdateResult> = either {
        val attributeInstances = ngsiLdAttributes.flatOnInstances()
        attributeInstances.parMap { (ngsiLdAttribute, ngsiLdAttributeInstance) ->
            logger.debug("Appending attribute {} in entity {}", ngsiLdAttribute.name, entityUri)
            val currentTea =
                getForEntityAndAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId)
                    .fold({ null }, { it })
            val attributeMetadata = ngsiLdAttributeInstance.toTemporalAttributeMetadata().bind()
            val attributePayload = getAttributeFromExpandedAttributes(
                expandedAttributes,
                ngsiLdAttribute.name,
                ngsiLdAttributeInstance.datasetId
            )!!
            if (currentTea == null) {
                addAttribute(
                    entityUri,
                    ngsiLdAttribute.name,
                    attributeMetadata,
                    createdAt,
                    attributePayload,
                    sub
                ).map {
                    UpdateAttributeResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        UpdateOperationResult.APPENDED,
                        null
                    )
                }.bind()
            } else if (disallowOverwrite) {
                val message = "Attribute already exists on $entityUri and overwrite is not allowed, ignoring"
                logger.info(message)
                UpdateAttributeResult(
                    ngsiLdAttribute.name,
                    ngsiLdAttributeInstance.datasetId,
                    UpdateOperationResult.IGNORED,
                    message
                ).right().bind()
            } else {
                replaceAttribute(
                    currentTea,
                    ngsiLdAttribute,
                    attributeMetadata,
                    createdAt,
                    attributePayload,
                    sub
                ).map {
                    UpdateAttributeResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        UpdateOperationResult.REPLACED,
                        null
                    )
                }.bind()
            }
        }
    }.fold({ it.left() }, { updateResultFromDetailedResult(it).right() })

    @Transactional
    suspend fun updateEntityAttributes(
        entityUri: URI,
        ngsiLdAttributes: List<NgsiLdAttribute>,
        expandedAttributes: ExpandedAttributes,
        createdAt: ZonedDateTime,
        sub: Sub?
    ): Either<APIException, UpdateResult> = either {
        val attributeInstances = ngsiLdAttributes.flatOnInstances()
        attributeInstances.parMap { (ngsiLdAttribute, ngsiLdAttributeInstance) ->
            logger.debug("Updating attribute {} in entity {}", ngsiLdAttribute.name, entityUri)
            val currentTea =
                getForEntityAndAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId)
                    .fold({ null }, { it })
            if (currentTea != null) {
                val attributeMetadata = ngsiLdAttributeInstance.toTemporalAttributeMetadata().bind()
                val attributePayload = getAttributeFromExpandedAttributes(
                    expandedAttributes,
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
                ).map {
                    UpdateAttributeResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        UpdateOperationResult.REPLACED,
                        null
                    )
                }.bind()
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
                ).right().bind()
            }
        }
    }.fold({ it.left() }, { updateResultFromDetailedResult(it).right() })

    @Transactional
    suspend fun partialUpdateEntityAttribute(
        entityId: URI,
        expandedAttribute: ExpandedAttribute,
        modifiedAt: ZonedDateTime,
        sub: Sub?
    ): Either<APIException, UpdateResult> = either {
        val attributeName = expandedAttribute.first
        val attributeValues = expandedAttribute.second[0]
        logger.debug(
            "Updating attribute {} of entity {} with values: {}",
            attributeName,
            entityId,
            attributeValues
        )

        val datasetId = attributeValues.getDatasetId()
        val exists = hasAttribute(entityId, attributeName, datasetId).bind()
        val updateAttributeResult =
            if (exists) {
                // first update payload in temporal entity attribute
                val tea = getForEntityAndAttribute(entityId, attributeName, datasetId).bind()
                val (jsonTargetObject, updatedAttributeInstance) = mergeAttributePayload(tea, attributeValues)
                val value = getValueFromPartialAttributePayload(tea, updatedAttributeInstance)
                val attributeValueType = guessAttributeValueType(tea.attributeType, attributeValues)
                updateOnUpdate(tea.id, attributeValueType, modifiedAt, jsonTargetObject.toString()).bind()

                // then update attribute instance
                val attributeInstance = createContextualAttributeInstance(
                    tea,
                    updatedAttributeInstance,
                    value,
                    modifiedAt,
                    sub
                )
                attributeInstanceService.create(attributeInstance).bind()

                UpdateAttributeResult(
                    attributeName,
                    datasetId,
                    UpdateOperationResult.UPDATED,
                    null
                )
            } else {
                UpdateAttributeResult(
                    attributeName,
                    datasetId,
                    UpdateOperationResult.IGNORED,
                    "Unknown attribute $attributeName with datasetId $datasetId in entity $entityId"
                )
            }

        updateResultFromDetailedResult(listOf(updateAttributeResult))
    }

    @Transactional
    suspend fun upsertEntityAttributes(
        entityUri: URI,
        ngsiLdAttribute: NgsiLdAttribute,
        expandedAttributes: ExpandedAttributes,
        createdAt: ZonedDateTime,
        sub: Sub?
    ): Either<APIException, Unit> = either {
        val ngsiLdAttributeInstance = ngsiLdAttribute.getAttributeInstances()[0]
        logger.debug("Upserting temporal attribute {} in entity {}", ngsiLdAttribute.name, entityUri)
        val currentTea =
            getForEntityAndAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId)
                .fold({ null }, { it })
        val attributeMetadata = ngsiLdAttributeInstance.toTemporalAttributeMetadata().bind()
        val attributePayload = getAttributeFromExpandedAttributes(
            expandedAttributes,
            ngsiLdAttribute.name,
            ngsiLdAttributeInstance.datasetId
        )!!

        if (currentTea == null) {
            logger.debug(
                "Creating attribute and instance for attribute {} in entity {}",
                ngsiLdAttribute.name,
                entityUri
            )
            addAttribute(
                entityUri,
                ngsiLdAttribute.name,
                attributeMetadata,
                createdAt,
                attributePayload,
                sub
            ).bind()
        } else {
            logger.debug("Adding instance to attribute {} to entity {}", currentTea.attributeName, entityUri)
            attributeInstanceService.addAttributeInstance(
                currentTea.id,
                attributeMetadata,
                expandedAttributes[currentTea.attributeName]!!.first()
            ).bind()
        }
    }

    @Transactional
    suspend fun mergeEntityAttributes(
        entityUri: URI,
        ngsiLdAttributes: List<NgsiLdAttribute>,
        expandedAttributes: ExpandedAttributes,
        createdAt: ZonedDateTime,
        observedAt: ZonedDateTime?,
        sub: Sub?
    ): Either<APIException, UpdateResult> = either {
        val attributeInstances = ngsiLdAttributes.flatOnInstances()
        attributeInstances.parMap { (ngsiLdAttribute, ngsiLdAttributeInstance) ->
            logger.debug("Merging attribute {} in entity {}", ngsiLdAttribute.name, entityUri)
            val currentTea =
                getForEntityAndAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId)
                    .fold({ null }, { it })
            val attributeMetadata = ngsiLdAttributeInstance.toTemporalAttributeMetadata().bind()
            val attributePayload = getAttributeFromExpandedAttributes(
                expandedAttributes,
                ngsiLdAttribute.name,
                ngsiLdAttributeInstance.datasetId
            )!!

            if (currentTea == null) {
                addAttribute(
                    entityUri,
                    ngsiLdAttribute.name,
                    attributeMetadata,
                    createdAt,
                    attributePayload,
                    sub
                ).map {
                    UpdateAttributeResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        UpdateOperationResult.APPENDED,
                        null
                    )
                }.bind()
            } else {
                mergeAttribute(
                    currentTea,
                    ngsiLdAttribute.name,
                    attributeMetadata,
                    createdAt,
                    observedAt,
                    attributePayload,
                    sub
                ).map {
                    UpdateAttributeResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        UpdateOperationResult.UPDATED,
                        null
                    )
                }.bind()
            }
        }
    }.fold({ it.left() }, { updateResultFromDetailedResult(it).right() })

    @Transactional
    suspend fun replaceEntityAttribute(
        entityId: URI,
        ngsiLdAttribute: NgsiLdAttribute,
        expandedAttribute: ExpandedAttribute,
        replacedAt: ZonedDateTime,
        sub: Sub?
    ): Either<APIException, UpdateResult> = either {
        val ngsiLdAttributeInstance = ngsiLdAttribute.getAttributeInstances()[0]
        val attributeName = ngsiLdAttribute.name
        val datasetId = ngsiLdAttributeInstance.datasetId
        val currentTea =
            getForEntityAndAttribute(entityId, attributeName, datasetId).fold({ null }, { it })
        val attributeMetadata = ngsiLdAttributeInstance.toTemporalAttributeMetadata().bind()
        val updateAttributeResult =
            if (currentTea == null) {
                UpdateAttributeResult(
                    attributeName,
                    datasetId,
                    UpdateOperationResult.IGNORED,
                    "Unknown attribute $attributeName with datasetId $datasetId in entity $entityId"
                )
            } else {
                replaceAttribute(
                    currentTea,
                    ngsiLdAttribute,
                    attributeMetadata,
                    replacedAt,
                    expandedAttribute.second.first(),
                    sub
                ).bind()

                UpdateAttributeResult(
                    ngsiLdAttribute.name,
                    ngsiLdAttributeInstance.datasetId,
                    UpdateOperationResult.REPLACED,
                    null
                )
            }

        updateResultFromDetailedResult(listOf(updateAttributeResult))
    }

    suspend fun getValueFromPartialAttributePayload(
        tea: TemporalEntityAttribute,
        attributePayload: ExpandedAttributeInstance
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

    suspend fun mergeAttributePayload(
        tea: TemporalEntityAttribute,
        expandedAttributeInstance: ExpandedAttributeInstance
    ): Pair<JSONObject, ExpandedAttributeInstance> {
        val jsonSourceObject = JSONObject(tea.payload.asString())
        val jsonUpdateObject = JSONObject(expandedAttributeInstance)
        val jsonMerger = JsonMerger(
            arrayMergeMode = JsonMerger.ArrayMergeMode.REPLACE_ARRAY,
            objectMergeMode = JsonMerger.ObjectMergeMode.MERGE_OBJECT
        )
        val jsonTargetObject = jsonMerger.merge(jsonSourceObject, jsonUpdateObject)
        val updatedAttributeInstance = jsonTargetObject.toMap() as ExpandedAttributeInstance
        return Pair(jsonTargetObject, updatedAttributeInstance)
    }

    private fun createContextualAttributeInstance(
        tea: TemporalEntityAttribute,
        expandedAttributeInstance: ExpandedAttributeInstance,
        value: Triple<String?, Double?, WKTCoordinates?>,
        modifiedAt: ZonedDateTime,
        sub: Sub?
    ): AttributeInstance {
        val timeAndProperty =
            if (expandedAttributeInstance.containsKey(NGSILD_OBSERVED_AT_PROPERTY))
                Pair(
                    getPropertyValueFromMapAsDateTime(expandedAttributeInstance, NGSILD_OBSERVED_AT_PROPERTY)!!,
                    AttributeInstance.TemporalProperty.OBSERVED_AT
                )
            else
                Pair(modifiedAt, AttributeInstance.TemporalProperty.MODIFIED_AT)

        return AttributeInstance(
            temporalEntityAttribute = tea.id,
            timeAndProperty = timeAndProperty,
            value = value,
            payload = expandedAttributeInstance,
            sub = sub
        )
    }

    /**
     * As specified in 5.6.17: If a common "observedAt" timestamp is defined and an existing Attribute to be merged
     * previously contained an "observedAt" sub-Attribute, the "observedAt" sub-Attribute is also updated using the
     * common timestamp, unless the Entity Fragment itself contains an explicit updated value for the
     * "observedAt" sub-Attribute.
     */
    internal fun processObservedAtInMergeOperation(
        tea: TemporalEntityAttribute,
        attributePayload: ExpandedAttributeInstance,
        attributeMetadata: AttributeMetadata,
        observedAt: ZonedDateTime?
    ): Pair<ExpandedAttributeInstance, AttributeMetadata> {
        return if (
            observedAt != null &&
            tea.payload.deserializeAsMap().containsKey(NGSILD_OBSERVED_AT_PROPERTY) &&
            !attributePayload.containsKey(NGSILD_OBSERVED_AT_PROPERTY)
        ) {
            Pair(
                attributePayload.plus(NGSILD_OBSERVED_AT_PROPERTY to buildNonReifiedDateTime(observedAt)),
                attributeMetadata.copy(observedAt = observedAt)
            )
        } else Pair(attributePayload, attributeMetadata)
    }
}
