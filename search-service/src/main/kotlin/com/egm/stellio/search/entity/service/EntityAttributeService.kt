package com.egm.stellio.search.entity.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.right
import arrow.fx.coroutines.parMap
import com.egm.stellio.search.common.util.allToMappedList
import com.egm.stellio.search.common.util.deserializeAsMap
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toDatasetIdFilter
import com.egm.stellio.search.common.util.toJson
import com.egm.stellio.search.common.util.toOptionalUri
import com.egm.stellio.search.common.util.toOptionalZonedDateTime
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.common.util.toUuid
import com.egm.stellio.search.common.util.toZonedDateTime
import com.egm.stellio.search.common.util.valueToDoubleOrNull
import com.egm.stellio.search.common.util.valueToStringOrNull
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.AttributeMetadata
import com.egm.stellio.search.entity.model.AttributeOperationResult
import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.search.entity.model.FailedAttributeOperationResult
import com.egm.stellio.search.entity.model.OperationStatus
import com.egm.stellio.search.entity.model.SucceededAttributeOperationResult
import com.egm.stellio.search.entity.util.guessAttributeValueType
import com.egm.stellio.search.entity.util.hasNgsiLdNullValue
import com.egm.stellio.search.entity.util.mergePatch
import com.egm.stellio.search.entity.util.partialUpdatePatch
import com.egm.stellio.search.entity.util.prepareAttributes
import com.egm.stellio.search.entity.util.toAttributeMetadata
import com.egm.stellio.search.entity.util.toExpandedAttributeInstance
import com.egm.stellio.search.temporal.model.AttributeInstance
import com.egm.stellio.search.temporal.service.AttributeInstanceService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedAttribute
import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.model.ExpandedAttributes
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.NgsiLdAttribute
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.model.addSysAttrs
import com.egm.stellio.shared.model.flatOnInstances
import com.egm.stellio.shared.model.getAttributeFromExpandedAttributes
import com.egm.stellio.shared.model.getDatasetId
import com.egm.stellio.shared.model.getMemberValue
import com.egm.stellio.shared.model.getMemberValueAsDateTime
import com.egm.stellio.shared.model.getPropertyValue
import com.egm.stellio.shared.model.isAttributeOfType
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.AttributeType
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_JSONPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LANGUAGEPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NONE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PREFIX
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_VOCABPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.attributeNotFoundMessage
import com.egm.stellio.shared.util.entityNotFoundMessage
import com.egm.stellio.shared.util.ngsiLdDateTime
import io.r2dbc.postgresql.codec.Json
import org.slf4j.LoggerFactory
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.ZonedDateTime
import java.util.*

@Service
class EntityAttributeService(
    private val databaseClient: DatabaseClient,
    private val attributeInstanceService: AttributeInstanceService,
    private val applicationProperties: ApplicationProperties
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    suspend fun upsert(attribute: Attribute): Either<APIException, UUID> =
        databaseClient.sql(
            """
            INSERT INTO temporal_entity_attribute
                (id, entity_id, attribute_name, attribute_type, attribute_value_type, created_at, modified_at, 
                    dataset_id, payload)
            VALUES 
                (:id, :entity_id, :attribute_name, :attribute_type, :attribute_value_type, :created_at, :created_at,
                    :dataset_id, :payload)
            ON CONFLICT (entity_id, attribute_name, dataset_id)
                DO UPDATE SET deleted_at = null,
                    attribute_type = :attribute_type,
                    attribute_value_type = :attribute_value_type,
                    modified_at = :created_at,
                    payload = :payload
            RETURNING id
            """.trimIndent()
        )
            .bind("id", attribute.id)
            .bind("entity_id", attribute.entityId)
            .bind("attribute_name", attribute.attributeName)
            .bind("attribute_type", attribute.attributeType.toString())
            .bind("attribute_value_type", attribute.attributeValueType.toString())
            .bind("created_at", attribute.createdAt)
            .bind("dataset_id", attribute.datasetId)
            .bind("payload", attribute.payload)
            .oneToResult { row -> toUuid(row["id"]) }

    @Transactional
    suspend fun update(
        attributeUUID: UUID,
        valueType: Attribute.AttributeValueType,
        modifiedAt: ZonedDateTime,
        payload: String
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET payload = :payload,
                attribute_value_type = :attribute_value_type,
                modified_at = :modified_at
            WHERE id = :attribute_uuid
            """.trimIndent()
        )
            .bind("attribute_uuid", attributeUUID)
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
    suspend fun createAttributes(
        payload: String,
        contexts: List<String>,
        sub: String? = null
    ): Either<APIException, Unit> = either {
        val createdAt = ngsiLdDateTime()
        val expandedEntity = expandJsonLdEntity(payload, contexts)
        val ngsiLdEntity = expandedEntity.toNgsiLdEntity().bind()
        ngsiLdEntity.prepareAttributes()
            .map {
                createAttributes(ngsiLdEntity, expandedEntity, it, createdAt, sub).bind()
            }.bind()
    }

    @Transactional
    suspend fun createAttributes(
        ngsiLdEntity: NgsiLdEntity,
        expandedEntity: ExpandedEntity,
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
                val attributePayload = expandedEntity.getAttributes().getAttributeFromExpandedAttributes(
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
    ): Either<APIException, Unit> = either {
        logger.debug("Adding attribute {} to entity {}", attributeName, entityId)
        val attribute = Attribute(
            entityId = entityId,
            attributeName = attributeName,
            attributeType = attributeMetadata.type,
            attributeValueType = attributeMetadata.valueType,
            datasetId = attributeMetadata.datasetId,
            createdAt = createdAt,
            payload = Json.of(serializeObject(attributePayload))
        )
        val attributeUuid = upsert(attribute).bind()

        // if the temporal property existed before, the create operation returned a different id than the one
        // in the attribute object
        val timeProperty =
            if (attributeUuid != attribute.id) AttributeInstance.TemporalProperty.MODIFIED_AT
            else AttributeInstance.TemporalProperty.CREATED_AT

        val attributeInstance = AttributeInstance(
            attributeUuid = attributeUuid,
            timeProperty = timeProperty,
            time = createdAt,
            attributeMetadata = attributeMetadata,
            payload = attributePayload,
            sub = sub
        )
        attributeInstanceService.create(attributeInstance).bind()

        if (attributeMetadata.observedAt != null) {
            val attributeObservedAtInstance = AttributeInstance(
                attributeUuid = attributeUuid,
                time = attributeMetadata.observedAt,
                attributeMetadata = attributeMetadata,
                payload = attributePayload
            )
            attributeInstanceService.create(attributeObservedAtInstance).bind()
        }
    }

    @Transactional
    suspend fun replaceAttribute(
        attribute: Attribute,
        ngsiLdAttribute: NgsiLdAttribute,
        attributeMetadata: AttributeMetadata,
        createdAt: ZonedDateTime,
        attributePayload: ExpandedAttributeInstance,
        sub: Sub?
    ): Either<APIException, Unit> = either {
        logger.debug(
            "Replacing attribute {} ({}) in entity {}",
            ngsiLdAttribute.name,
            attributeMetadata.datasetId,
            attribute.entityId
        )
        addAttribute(
            attribute.entityId,
            attribute.attributeName,
            attributeMetadata,
            createdAt,
            attributePayload,
            sub
        ).bind()
    }

    @Transactional
    suspend fun mergeAttribute(
        attribute: Attribute,
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
            attribute.entityId
        )
        val (processedAttributePayload, processedAttributeMetadata) = processObservedAtInMergeOperation(
            attribute,
            attributePayload,
            attributeMetadata,
            observedAt
        )
        val (jsonTargetObject, updatedAttributeInstance) =
            mergePatch(attribute.payload.toExpandedAttributeInstance(), processedAttributePayload)
        val value = getValueFromPartialAttributePayload(attribute, updatedAttributeInstance)
        update(attribute.id, processedAttributeMetadata.valueType, mergedAt, jsonTargetObject).bind()

        val attributeInstance =
            createContextualAttributeInstance(attribute, updatedAttributeInstance, value, mergedAt, sub)
        attributeInstanceService.create(attributeInstance).bind()
    }

    @Transactional
    suspend fun deleteAttributes(entityId: URI, deletedAt: ZonedDateTime): Either<APIException, Unit> = either {
        val attributesToDelete = getForEntity(entityId, emptySet(), emptySet())
        deleteSelectedAttributes(attributesToDelete, deletedAt).bind()
    }

    @Transactional
    suspend fun deleteAttribute(
        entityId: URI,
        attributeName: String,
        datasetId: URI?,
        deleteAll: Boolean = false,
        deletedAt: ZonedDateTime
    ): Either<APIException, List<SucceededAttributeOperationResult>> = either {
        logger.debug("Deleting attribute {} from entity {} (all: {})", attributeName, entityId, deleteAll)
        val attributesToDelete =
            if (deleteAll)
                getForEntity(entityId, setOf(attributeName), emptySet())
            else
                listOf(getForEntityAndAttribute(entityId, attributeName, datasetId).bind())

        deleteSelectedAttributes(attributesToDelete, deletedAt).bind()
            .map { expandedAttributeInstance ->
                SucceededAttributeOperationResult(
                    attributeName,
                    datasetId,
                    OperationStatus.DELETED,
                    expandedAttributeInstance
                )
            }
    }

    @Transactional
    internal suspend fun deleteSelectedAttributes(
        attributesToDelete: List<Attribute>,
        deletedAt: ZonedDateTime
    ): Either<APIException, List<ExpandedAttributeInstance>> = either {
        if (attributesToDelete.isEmpty()) return emptyList<ExpandedAttributeInstance>().right()
        val attributesToDeleteWithPayload = attributesToDelete.map {
            Pair(
                it,
                JsonLdUtils.expandAttribute(
                    it.attributeName,
                    it.attributeType.toNullCompactedRepresentation(it.datasetId),
                    listOf(applicationProperties.contexts.core)
                ).second[0]
            )
        }

        val teasTimestamps = databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET deleted_at = new.deleted_at,
                payload = new.payload
            FROM (VALUES :values) AS new(uuid, deleted_at, payload)
            WHERE temporal_entity_attribute.id = new.uuid
            RETURNING id, created_at, modified_at, new.deleted_at
            """.trimIndent()
        )
            .bind("values", attributesToDeleteWithPayload.map { arrayOf(it.first.id, deletedAt, it.second.toJson()) })
            .allToMappedList { row ->
                mapOf(
                    toUuid(row["id"]) to Triple(
                        toZonedDateTime(row["created_at"]),
                        toOptionalZonedDateTime(row["modified_at"]),
                        toZonedDateTime(row["deleted_at"])
                    )
                )
            }

        attributesToDeleteWithPayload.forEach { (attribute, expandedAttributePayload) ->
            attributeInstanceService.addDeletedAttributeInstance(
                attributeUuid = attribute.id,
                value = attribute.attributeType.toNullValue(),
                deletedAt = deletedAt,
                attributeValues = expandedAttributePayload
            ).bind()
        }

        attributesToDeleteWithPayload.map { (attribute, expandedAttributeInstance) ->
            val teaTimestamps = teasTimestamps.find { it.containsKey(attribute.id) }!!.values.first()
            expandedAttributeInstance.addSysAttrs(true, teaTimestamps.first, teaTimestamps.second, teaTimestamps.third)
        }
    }

    @Transactional
    suspend fun permanentlyDeleteAttribute(
        entityId: URI,
        attributeName: String,
        datasetId: URI?,
        deleteAll: Boolean = false
    ): Either<APIException, Unit> = either {
        logger.debug("Permanently deleting attribute {} from entity {} (all: {})", attributeName, entityId, deleteAll)
        val attributesToDelete =
            if (deleteAll)
                getForEntity(entityId, setOf(attributeName), emptySet(), false)
            else
                listOf(getForEntityAndAttribute(entityId, attributeName, datasetId).bind())

        databaseClient.sql(
            """
            DELETE FROM temporal_entity_attribute
            WHERE id IN(:uuids)
            """.trimIndent()
        )
            .bind("uuids", attributesToDelete.map { it.id })
            .execute()

        if (deleteAll)
            attributeInstanceService.deleteAllInstancesOfAttribute(entityId, attributeName).bind()
        else
            attributeInstanceService.deleteInstancesOfAttribute(entityId, attributeName, datasetId).bind()
    }

    @Transactional
    suspend fun permanentlyDeleteAttributes(
        entityId: URI,
    ): Either<APIException, Unit> = either {
        logger.debug("Permanently deleting all attributes from entity {}", entityId)

        val deletedTeas = databaseClient.sql(
            """
            DELETE FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            RETURNING id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .allToMappedList { toUuid(it["id"]) }

        if (deletedTeas.isNotEmpty())
            attributeInstanceService.deleteInstancesOfEntity(deletedTeas).bind()
        else Unit.right()
    }

    suspend fun getForEntities(
        entitiesIds: List<URI>,
        entitiesQuery: EntitiesQuery
    ): List<Attribute> {
        val filterOnAttributes =
            if (entitiesQuery.attrs.isNotEmpty())
                " AND " + entitiesQuery.attrs.joinToString(
                    separator = ",",
                    prefix = "attribute_name in (",
                    postfix = ")"
                ) { "'$it'" }
            else ""

        val filterOnDatasetId =
            if (entitiesQuery.datasetId.isNotEmpty()) {
                val datasetIdsList = entitiesQuery.datasetId.joinToString(",") { "'$it'" }
                " AND ((dataset_id IS NOT NULL AND dataset_id in ($datasetIdsList)) " +
                    "OR (dataset_id IS NULL AND '$NGSILD_NONE_TERM' in ($datasetIdsList)))"
            } else ""

        val selectQuery =
            """
            SELECT id, entity_id, attribute_name, attribute_type, attribute_value_type, created_at, modified_at,
                deleted_at, dataset_id, payload
            FROM temporal_entity_attribute            
            WHERE entity_id IN (:entities_ids) 
            $filterOnAttributes
            $filterOnDatasetId
            ORDER BY entity_id
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entities_ids", entitiesIds)
            .allToMappedList { rowToAttribute(it) }
    }

    suspend fun getForEntity(
        id: URI,
        attrs: Set<String>,
        datasetIds: Set<String>,
        excludeDeleted: Boolean = true
    ): List<Attribute> {
        val filterOnAttributes =
            if (attrs.isNotEmpty())
                " AND " + attrs.joinToString(
                    separator = ",",
                    prefix = "attribute_name in (",
                    postfix = ")"
                ) { "'$it'" }
            else ""

        val filterOnDatasetId =
            if (datasetIds.isNotEmpty()) {
                val datasetIdsList = datasetIds.joinToString(",") { "'$it'" }
                " AND ((dataset_id IS NOT NULL AND dataset_id in ($datasetIdsList)) " +
                    "OR (dataset_id IS NULL AND '@none' in ($datasetIdsList)))"
            } else ""

        val selectQuery =
            """
            SELECT id, entity_id, attribute_name, attribute_type, attribute_value_type, created_at, modified_at, 
                dataset_id, payload
            FROM temporal_entity_attribute            
            WHERE entity_id = :entity_id
            ${if (excludeDeleted) " and deleted_at is null " else ""}
            $filterOnAttributes
            $filterOnDatasetId
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", id)
            .allToMappedList { rowToAttribute(it) }
    }

    suspend fun getForEntityAndAttribute(
        id: URI,
        attributeName: String,
        datasetId: URI? = null
    ): Either<APIException, Attribute> {
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
                rowToAttribute(it)
            }
    }

    private fun rowToAttribute(row: Map<String, Any>) =
        Attribute(
            id = toUuid(row["id"]),
            entityId = toUri(row["entity_id"]),
            attributeName = row["attribute_name"] as ExpandedTerm,
            attributeType = Attribute.AttributeType.valueOf(row["attribute_type"] as String),
            attributeValueType = Attribute.AttributeValueType.valueOf(
                row["attribute_value_type"] as String
            ),
            datasetId = toOptionalUri(row["dataset_id"]),
            createdAt = toZonedDateTime(row["created_at"]),
            modifiedAt = toZonedDateTime(row["modified_at"]),
            deletedAt = toOptionalZonedDateTime(row["deleted_at"]),
            payload = toJson(row["payload"])
        )

    suspend fun checkEntityAndAttributeExistence(
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI? = null,
        anyAttributeInstance: Boolean = false
    ): Either<APIException, Unit> {
        val datasetIdFilter =
            if (anyAttributeInstance) ""
            else datasetId.toDatasetIdFilter()
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
                    and deleted_at is null
                    $datasetIdFilter
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
                if (it.first)
                    if (it.second)
                        Unit.right()
                    else ResourceNotFoundException(attributeNotFoundMessage(attributeName, datasetId)).left()
                else ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()
            }
    }

    @Transactional
    suspend fun appendAttributes(
        entityUri: URI,
        ngsiLdAttributes: List<NgsiLdAttribute>,
        expandedAttributes: ExpandedAttributes,
        disallowOverwrite: Boolean,
        createdAt: ZonedDateTime,
        sub: Sub?
    ): Either<APIException, List<SucceededAttributeOperationResult>> = either {
        val attributeInstances = ngsiLdAttributes.flatOnInstances()
        attributeInstances.parMap { (ngsiLdAttribute, ngsiLdAttributeInstance) ->
            logger.debug("Appending attribute {} in entity {}", ngsiLdAttribute.name, entityUri)
            val currentAttribute =
                getForEntityAndAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId)
                    .fold({ null }, { it })
            val attributeMetadata = ngsiLdAttributeInstance.toAttributeMetadata().bind()
            val attributePayload = expandedAttributes.getAttributeFromExpandedAttributes(
                ngsiLdAttribute.name,
                ngsiLdAttributeInstance.datasetId
            )!!
            if (currentAttribute == null || currentAttribute.deletedAt != null) {
                addAttribute(
                    entityUri,
                    ngsiLdAttribute.name,
                    attributeMetadata,
                    createdAt,
                    attributePayload,
                    sub
                ).map {
                    SucceededAttributeOperationResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        OperationStatus.CREATED,
                        attributePayload
                    )
                }.bind()
            } else if (disallowOverwrite) {
                logger.info("Attribute already exists on $entityUri and overwrite is not allowed, ignoring")
                SucceededAttributeOperationResult(
                    ngsiLdAttribute.name,
                    ngsiLdAttributeInstance.datasetId,
                    OperationStatus.IGNORED,
                    attributePayload
                ).right().bind()
            } else {
                replaceAttribute(
                    currentAttribute,
                    ngsiLdAttribute,
                    attributeMetadata,
                    createdAt,
                    attributePayload,
                    sub
                ).map {
                    SucceededAttributeOperationResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        OperationStatus.UPDATED,
                        attributePayload
                    )
                }.bind()
            }
        }
    }.fold({ it.left() }, { it.right() })

    @Transactional
    suspend fun updateAttributes(
        entityUri: URI,
        ngsiLdAttributes: List<NgsiLdAttribute>,
        expandedAttributes: ExpandedAttributes,
        createdAt: ZonedDateTime,
        sub: Sub?
    ): Either<APIException, List<SucceededAttributeOperationResult>> = either {
        val attributeInstances = ngsiLdAttributes.flatOnInstances()
        attributeInstances.parMap { (ngsiLdAttribute, ngsiLdAttributeInstance) ->
            logger.debug("Updating attribute {} in entity {}", ngsiLdAttribute.name, entityUri)
            val currentAttribute =
                getForEntityAndAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId)
                    .fold({ null }, { it })
            val attributeMetadata = ngsiLdAttributeInstance.toAttributeMetadata().bind()
            val attributePayload = expandedAttributes.getAttributeFromExpandedAttributes(
                ngsiLdAttribute.name,
                ngsiLdAttributeInstance.datasetId
            )!!

            if (currentAttribute == null || currentAttribute.deletedAt != null) {
                addAttribute(
                    entityUri,
                    ngsiLdAttribute.name,
                    attributeMetadata,
                    createdAt,
                    attributePayload,
                    sub
                ).map {
                    SucceededAttributeOperationResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        OperationStatus.CREATED,
                        attributePayload
                    )
                }.bind()
            } else if (hasNgsiLdNullValue(attributePayload, currentAttribute.attributeType)) {
                deleteAttribute(
                    entityUri,
                    ngsiLdAttribute.name,
                    ngsiLdAttributeInstance.datasetId,
                    false,
                    createdAt
                ).bind().first()
            } else {
                replaceAttribute(
                    currentAttribute,
                    ngsiLdAttribute,
                    attributeMetadata,
                    createdAt,
                    attributePayload,
                    sub
                ).map {
                    SucceededAttributeOperationResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        OperationStatus.UPDATED,
                        attributePayload
                    )
                }.bind()
            }
        }
    }.fold({ it.left() }, { it.right() })

    @Transactional
    suspend fun partialUpdateAttribute(
        entityId: URI,
        expandedAttribute: ExpandedAttribute,
        modifiedAt: ZonedDateTime,
        sub: Sub?
    ): Either<APIException, AttributeOperationResult> = either {
        val attributeName = expandedAttribute.first
        val attributeValues = expandedAttribute.second[0]
        logger.debug("Partial updating attribute {} in entity {}", attributeName, entityId)

        val datasetId = attributeValues.getDatasetId()
        val currentAttribute = getForEntityAndAttribute(entityId, attributeName, datasetId).fold({ null }, { it })
        val attributeOperationResult =
            if (currentAttribute == null || currentAttribute.deletedAt != null) {
                FailedAttributeOperationResult(
                    attributeName,
                    datasetId,
                    OperationStatus.FAILED,
                    "Unknown attribute $attributeName with datasetId $datasetId in entity $entityId"
                )
            } else if (hasNgsiLdNullValue(attributeValues, currentAttribute.attributeType)) {
                deleteAttribute(
                    entityId,
                    attributeName,
                    datasetId,
                    false,
                    modifiedAt
                ).bind().first()
            } else {
                applyPartialUpdatePatchOperation(
                    currentAttribute,
                    attributeValues,
                    modifiedAt,
                    sub
                ).bind()
            }

        attributeOperationResult
    }

    @Transactional
    internal suspend fun applyPartialUpdatePatchOperation(
        attribute: Attribute,
        attributeValues: ExpandedAttributeInstance,
        modifiedAt: ZonedDateTime,
        sub: Sub?
    ): Either<APIException, SucceededAttributeOperationResult> = either {
        // first update payload in temporal entity attribute
        attributeValues[JSONLD_TYPE]?.let {
            ensure(isAttributeOfType(attributeValues, AttributeType(NGSILD_PREFIX + attribute.attributeType))) {
                BadRequestDataException("The type of the attribute has to be the same as the existing one")
            }
        }
        val (jsonTargetObject, updatedAttributeInstance) =
            partialUpdatePatch(attribute.payload.toExpandedAttributeInstance(), attributeValues)
        val value = getValueFromPartialAttributePayload(attribute, updatedAttributeInstance)
        val attributeValueType = guessAttributeValueType(attribute.attributeType, attributeValues)
        update(attribute.id, attributeValueType, modifiedAt, jsonTargetObject).bind()

        // then update attribute instance
        val attributeInstance = createContextualAttributeInstance(
            attribute,
            updatedAttributeInstance,
            value,
            modifiedAt,
            sub
        )
        attributeInstanceService.create(attributeInstance).bind()

        SucceededAttributeOperationResult(
            attribute.attributeName,
            attribute.datasetId,
            OperationStatus.UPDATED,
            updatedAttributeInstance
        )
    }

    @Transactional
    suspend fun upsertAttributes(
        entityUri: URI,
        ngsiLdAttribute: NgsiLdAttribute,
        expandedAttributes: ExpandedAttributes,
        createdAt: ZonedDateTime,
        sub: Sub?
    ): Either<APIException, Unit> = either {
        val ngsiLdAttributeInstance = ngsiLdAttribute.getAttributeInstances()[0]
        logger.debug("Upserting temporal attribute {} in entity {}", ngsiLdAttribute.name, entityUri)
        val currentAttribute =
            getForEntityAndAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId)
                .fold({ null }, { it })
        val attributeMetadata = ngsiLdAttributeInstance.toAttributeMetadata().bind()
        val attributePayload = expandedAttributes.getAttributeFromExpandedAttributes(
            ngsiLdAttribute.name,
            ngsiLdAttributeInstance.datasetId
        )!!

        if (currentAttribute == null) {
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
            logger.debug("Adding instance to attribute {} to entity {}", currentAttribute.attributeName, entityUri)
            attributeInstanceService.addObservedAttributeInstance(
                currentAttribute.id,
                attributeMetadata,
                expandedAttributes[currentAttribute.attributeName]!!.first()
            ).bind()
        }
    }

    @Transactional
    suspend fun mergeAttributes(
        entityUri: URI,
        ngsiLdAttributes: List<NgsiLdAttribute>,
        expandedAttributes: ExpandedAttributes,
        createdAt: ZonedDateTime,
        observedAt: ZonedDateTime?,
        sub: Sub?
    ): Either<APIException, List<SucceededAttributeOperationResult>> = either {
        val attributeInstances = ngsiLdAttributes.flatOnInstances()
        attributeInstances.parMap { (ngsiLdAttribute, ngsiLdAttributeInstance) ->
            logger.debug("Merging attribute {} in entity {}", ngsiLdAttribute.name, entityUri)
            val currentAttribute =
                getForEntityAndAttribute(entityUri, ngsiLdAttribute.name, ngsiLdAttributeInstance.datasetId)
                    .fold({ null }, { it })
            val attributeMetadata = ngsiLdAttributeInstance.toAttributeMetadata().bind()
            val attributePayload = expandedAttributes.getAttributeFromExpandedAttributes(
                ngsiLdAttribute.name,
                ngsiLdAttributeInstance.datasetId
            )!!

            if (currentAttribute == null || currentAttribute.deletedAt != null)
                addAttribute(
                    entityUri,
                    ngsiLdAttribute.name,
                    attributeMetadata,
                    createdAt,
                    attributePayload,
                    sub
                ).map {
                    SucceededAttributeOperationResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        OperationStatus.CREATED,
                        attributePayload
                    )
                }.bind()
            else if (hasNgsiLdNullValue(attributePayload, currentAttribute.attributeType))
                deleteAttribute(
                    entityUri,
                    ngsiLdAttribute.name,
                    ngsiLdAttributeInstance.datasetId,
                    false,
                    createdAt
                ).bind().first()
            else
                mergeAttribute(
                    currentAttribute,
                    ngsiLdAttribute.name,
                    attributeMetadata,
                    createdAt,
                    observedAt,
                    attributePayload,
                    sub
                ).map {
                    SucceededAttributeOperationResult(
                        ngsiLdAttribute.name,
                        ngsiLdAttributeInstance.datasetId,
                        OperationStatus.UPDATED,
                        attributePayload
                    )
                }.bind()
        }
    }.fold({ it.left() }, { it.right() })

    @Transactional
    suspend fun replaceAttribute(
        entityId: URI,
        ngsiLdAttribute: NgsiLdAttribute,
        expandedAttribute: ExpandedAttribute,
        replacedAt: ZonedDateTime,
        sub: Sub?
    ): Either<APIException, AttributeOperationResult> = either {
        val ngsiLdAttributeInstance = ngsiLdAttribute.getAttributeInstances()[0]
        val attributeName = ngsiLdAttribute.name
        val datasetId = ngsiLdAttributeInstance.datasetId
        val currentAttribute = getForEntityAndAttribute(entityId, attributeName, datasetId).fold({ null }, { it })
        val attributeMetadata = ngsiLdAttributeInstance.toAttributeMetadata().bind()
        val attributeOperationResult =
            if (currentAttribute == null || currentAttribute.deletedAt != null) {
                FailedAttributeOperationResult(
                    attributeName,
                    datasetId,
                    OperationStatus.FAILED,
                    "Unknown attribute $attributeName with datasetId $datasetId in entity $entityId"
                )
            } else {
                replaceAttribute(
                    currentAttribute,
                    ngsiLdAttribute,
                    attributeMetadata,
                    replacedAt,
                    expandedAttribute.second.first(),
                    sub
                ).bind()

                SucceededAttributeOperationResult(
                    ngsiLdAttribute.name,
                    ngsiLdAttributeInstance.datasetId,
                    OperationStatus.UPDATED,
                    expandedAttribute.second.first()
                )
            }

        attributeOperationResult
    }

    suspend fun getValueFromPartialAttributePayload(
        attribute: Attribute,
        attributePayload: ExpandedAttributeInstance
    ): Triple<String?, Double?, WKTCoordinates?> =
        when (attribute.attributeType) {
            Attribute.AttributeType.Property ->
                Triple(
                    valueToStringOrNull(attributePayload.getPropertyValue()!!),
                    valueToDoubleOrNull(attributePayload.getPropertyValue()!!),
                    null
                )
            Attribute.AttributeType.Relationship ->
                Triple(
                    attributePayload.getMemberValue(NGSILD_RELATIONSHIP_OBJECT)!! as String,
                    null,
                    null
                )
            Attribute.AttributeType.GeoProperty ->
                Triple(
                    null,
                    null,
                    WKTCoordinates(attributePayload.getPropertyValue()!! as String)
                )
            Attribute.AttributeType.JsonProperty ->
                Triple(
                    serializeObject(attributePayload.getMemberValue(NGSILD_JSONPROPERTY_VALUE)!!),
                    null,
                    null
                )
            Attribute.AttributeType.LanguageProperty ->
                Triple(
                    serializeObject(attributePayload.getMemberValue(NGSILD_LANGUAGEPROPERTY_VALUE)!!),
                    null,
                    null
                )
            Attribute.AttributeType.VocabProperty ->
                Triple(
                    serializeObject(attributePayload.getMemberValue(NGSILD_VOCABPROPERTY_VALUE)!!),
                    null,
                    null
                )
        }

    private fun createContextualAttributeInstance(
        attribute: Attribute,
        expandedAttributeInstance: ExpandedAttributeInstance,
        value: Triple<String?, Double?, WKTCoordinates?>,
        modifiedAt: ZonedDateTime,
        sub: Sub?
    ): AttributeInstance {
        val timeAndProperty =
            if (expandedAttributeInstance.containsKey(NGSILD_OBSERVED_AT_PROPERTY))
                Pair(
                    expandedAttributeInstance.getMemberValueAsDateTime(NGSILD_OBSERVED_AT_PROPERTY)!!,
                    AttributeInstance.TemporalProperty.OBSERVED_AT
                )
            else
                Pair(modifiedAt, AttributeInstance.TemporalProperty.MODIFIED_AT)

        return AttributeInstance(
            attributeUuid = attribute.id,
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
        attribute: Attribute,
        attributePayload: ExpandedAttributeInstance,
        attributeMetadata: AttributeMetadata,
        observedAt: ZonedDateTime?
    ): Pair<ExpandedAttributeInstance, AttributeMetadata> =
        if (
            observedAt != null &&
            attribute.payload.deserializeAsMap().containsKey(NGSILD_OBSERVED_AT_PROPERTY) &&
            !attributePayload.containsKey(NGSILD_OBSERVED_AT_PROPERTY)
        )
            Pair(
                attributePayload.plus(NGSILD_OBSERVED_AT_PROPERTY to buildNonReifiedTemporalValue(observedAt)),
                attributeMetadata.copy(observedAt = observedAt)
            )
        else Pair(attributePayload, attributeMetadata)
}
