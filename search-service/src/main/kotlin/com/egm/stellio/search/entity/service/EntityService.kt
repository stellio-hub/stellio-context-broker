package com.egm.stellio.search.entity.service

import arrow.core.Either
import arrow.core.Either.Left
import arrow.core.Either.Right
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import arrow.core.toOption
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.common.util.deserializeAsMap
import com.egm.stellio.search.common.util.deserializeExpandedPayload
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toZonedDateTime
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.AttributeOperationResult
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.model.FailedAttributeOperationResult
import com.egm.stellio.search.entity.model.OperationStatus
import com.egm.stellio.search.entity.model.OperationType
import com.egm.stellio.search.entity.model.OperationType.APPEND_ATTRIBUTES
import com.egm.stellio.search.entity.model.OperationType.APPEND_ATTRIBUTES_OVERWRITE_ALLOWED
import com.egm.stellio.search.entity.model.OperationType.MERGE_ENTITY
import com.egm.stellio.search.entity.model.OperationType.UPDATE_ATTRIBUTES
import com.egm.stellio.search.entity.model.SucceededAttributeOperationResult
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.model.getSucceededOperations
import com.egm.stellio.search.entity.model.hasSuccessfulResult
import com.egm.stellio.search.entity.util.prepareAttributes
import com.egm.stellio.search.entity.util.rowToEntity
import com.egm.stellio.search.scope.ScopeService
import com.egm.stellio.search.temporal.model.AttributeInstance.TemporalProperty
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ExpandedAttribute
import com.egm.stellio.shared.model.ExpandedAttributeInstances
import com.egm.stellio.shared.model.ExpandedAttributes
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.addSysAttrs
import com.egm.stellio.shared.model.flattenOnAttributeAndDatasetId
import com.egm.stellio.shared.model.toNgsiLdAttributes
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_EXPANDED_ENTITY_SPECIFIC_MEMBERS
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_PROPERTY
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.entityAlreadyExistsMessage
import com.egm.stellio.shared.util.getSpecificAccessPolicy
import com.egm.stellio.shared.util.ngsiLdDateTime
import io.r2dbc.postgresql.codec.Json
import org.slf4j.LoggerFactory
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.ZonedDateTime

@Service
class EntityService(
    private val databaseClient: DatabaseClient,
    private val entityQueryService: EntityQueryService,
    private val entityAttributeService: EntityAttributeService,
    private val scopeService: ScopeService,
    private val entityEventService: EntityEventService,
    private val authorizationService: AuthorizationService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    suspend fun createEntity(
        ngsiLdEntity: NgsiLdEntity,
        expandedEntity: ExpandedEntity,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        entityQueryService.isMarkedAsDeleted(ngsiLdEntity.id).let {
            when (it) {
                is Left -> authorizationService.userCanCreateEntities(sub.toOption()).bind()
                is Right ->
                    if (!it.value)
                        AlreadyExistsException(entityAlreadyExistsMessage(ngsiLdEntity.id.toString())).left().bind()
                    else
                        authorizationService.userCanAdminEntity(ngsiLdEntity.id, sub.toOption()).bind()
            }
        }

        val createdAt = ngsiLdDateTime()
        val attributesMetadata = ngsiLdEntity.prepareAttributes().bind()
        logger.debug("Creating entity {}", ngsiLdEntity.id)

        createEntityPayload(ngsiLdEntity, expandedEntity, createdAt).bind()
        scopeService.createHistory(ngsiLdEntity, createdAt, sub).bind()
        val attrsOperationResult = entityAttributeService.createAttributes(
            ngsiLdEntity,
            expandedEntity,
            attributesMetadata,
            createdAt,
            sub
        ).bind()
        authorizationService.createOwnerRight(ngsiLdEntity.id, sub.toOption()).bind()

        entityEventService.publishEntityCreateEvent(
            sub,
            ngsiLdEntity.id,
            ngsiLdEntity.types
        )
        entityEventService.publishAttributeChangeEvents(
            sub,
            ngsiLdEntity.id,
            attrsOperationResult.getSucceededOperations()
        )
    }

    @Transactional
    suspend fun createEntityPayload(
        ngsiLdEntity: NgsiLdEntity,
        expandedEntity: ExpandedEntity,
        createdAt: ZonedDateTime
    ): Either<APIException, Unit> = either {
        val specificAccessPolicy = ngsiLdEntity.getSpecificAccessPolicy()?.bind()
        databaseClient.sql(
            """
            INSERT INTO entity_payload
                (entity_id, types, scopes, created_at, modified_at, payload, specific_access_policy)
            VALUES
                (:entity_id, :types, :scopes, :created_at, :created_at ,:payload, :specific_access_policy)
            ON CONFLICT (entity_id)
                DO UPDATE SET types = :types,
                    scopes = :scopes,
                    modified_at = :created_at,
                    deleted_at = null,
                    payload = :payload,
                    specific_access_policy = :specific_access_policy
            """.trimIndent()
        )
            .bind("entity_id", ngsiLdEntity.id)
            .bind("types", ngsiLdEntity.types.toTypedArray())
            .bind("scopes", ngsiLdEntity.scopes?.toTypedArray())
            .bind("created_at", createdAt)
            .bind("payload", Json.of(serializeObject(expandedEntity.populateCreationTimeDate(createdAt).members)))
            .bind("specific_access_policy", specificAccessPolicy?.toString())
            .execute()
    }

    @Transactional
    suspend fun mergeEntity(
        entityId: URI,
        expandedAttributes: ExpandedAttributes,
        observedAt: ZonedDateTime?,
        sub: Sub? = null
    ): Either<APIException, UpdateResult> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()

        val (coreAttrs, otherAttrs) =
            expandedAttributes.toList()
                // remove @id if it is present (optional as per 5.4)
                .filter { it.first != JSONLD_ID }
                .partition { JSONLD_EXPANDED_ENTITY_SPECIFIC_MEMBERS.contains(it.first) }
        val mergedAt = ngsiLdDateTime()
        logger.debug("Merging entity {}", entityId)

        val coreOperationResult = updateCoreAttributes(entityId, coreAttrs, mergedAt, MERGE_ENTITY).bind()
        val attrsOperationResult = entityAttributeService.mergeAttributes(
            entityId,
            otherAttrs.toMap().toNgsiLdAttributes().bind(),
            expandedAttributes,
            mergedAt,
            observedAt,
            sub
        ).bind()

        val operationResult = coreOperationResult.plus(attrsOperationResult)
        handleSuccessOperationActions(operationResult, entityId, mergedAt, sub).bind()

        UpdateResult(operationResult)
    }

    @Transactional
    suspend fun replaceEntity(
        entityId: URI,
        ngsiLdEntity: NgsiLdEntity,
        expandedEntity: ExpandedEntity,
        sub: Sub? = null
    ): Either<APIException, UpdateResult> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()

        logger.debug("Replacing entity {}", ngsiLdEntity.id)

        val replacedAt = ngsiLdDateTime()
        val currentEntityAttributes = entityQueryService.retrieve(entityId).bind()
            .toExpandedEntity()
            .getAttributes()
            .flattenOnAttributeAndDatasetId()
        val newEntityAttributes = expandedEntity
            .getAttributes()
            .flattenOnAttributeAndDatasetId()

        // all attributes not present in the new entity are to be deleted
        val attributesToDelete = currentEntityAttributes.filter { (currentAttributeName, currentDatasetId, _) ->
            newEntityAttributes.none { (newAttributeName, newDatasetId, _) ->
                currentAttributeName == newAttributeName && currentDatasetId == newDatasetId
            }
        }

        val deleteOperationResults = attributesToDelete.flatMap { (attributeName, datasetId, _) ->
            entityAttributeService.deleteAttribute(
                entityId,
                attributeName,
                datasetId,
                false,
                replacedAt
            ).bind()
        }

        // all attributes in the new entity are to be created or updated (if they already exist)
        // both create or update operations are handled by the appendAttributes
        val createOrReplaceOperationResult = newEntityAttributes
            .flatMap { (attributeName, _, expandedAttributeInstance) ->
                val expandedAttributes = mapOf(attributeName to listOf(expandedAttributeInstance))
                entityAttributeService.appendAttributes(
                    entityId,
                    expandedAttributes.toNgsiLdAttributes().bind(),
                    expandedAttributes,
                    false,
                    replacedAt,
                    sub
                ).bind()
            }

        replaceEntityPayload(ngsiLdEntity, expandedEntity, replacedAt).bind()
        scopeService.replace(ngsiLdEntity, replacedAt, sub).bind()

        val operationResult = deleteOperationResults.plus(createOrReplaceOperationResult)
        operationResult.filterIsInstance<SucceededAttributeOperationResult>()
            .forEach {
                if (it.operationStatus == OperationStatus.DELETED)
                    entityEventService.publishAttributeDeleteEvent(sub, entityId, it)
                else
                    entityEventService.publishAttributeChangeEvents(sub, entityId, listOf(it))
            }

        UpdateResult(operationResult)
    }

    @Transactional
    suspend fun replaceEntityPayload(
        ngsiLdEntity: NgsiLdEntity,
        expandedEntity: ExpandedEntity,
        replacedAt: ZonedDateTime
    ): Either<APIException, Unit> = either {
        val specificAccessPolicy = ngsiLdEntity.getSpecificAccessPolicy()?.bind()
        val createdAt = retrieveCreatedAt(ngsiLdEntity.id).bind()
        val serializedPayload =
            serializeObject(expandedEntity.populateReplacementTimeDates(createdAt, replacedAt).members)

        databaseClient.sql(
            """
            UPDATE entity_payload
            SET types = :types,
                scopes = :scopes,
                modified_at = :modified_at,
                payload = :payload,
                specific_access_policy = :specific_access_policy
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", ngsiLdEntity.id)
            .bind("types", ngsiLdEntity.types.toTypedArray())
            .bind("scopes", ngsiLdEntity.scopes?.toTypedArray())
            .bind("modified_at", replacedAt)
            .bind("payload", Json.of(serializedPayload))
            .bind("specific_access_policy", specificAccessPolicy?.toString())
            .execute()
    }

    private suspend fun retrieveCreatedAt(entityId: URI): Either<APIException, ZonedDateTime> =
        databaseClient.sql(
            """
            SELECT created_at from entity_payload
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .oneToResult { toZonedDateTime(it["created_at"]) }

    @Transactional
    suspend fun updateCoreAttributes(
        entityId: URI,
        coreAttrs: List<Pair<ExpandedTerm, ExpandedAttributeInstances>>,
        modifiedAt: ZonedDateTime,
        operationType: OperationType
    ): Either<APIException, List<AttributeOperationResult>> = either {
        coreAttrs.map { (expandedTerm, expandedAttributeInstances) ->
            when (expandedTerm) {
                JSONLD_TYPE ->
                    updateTypes(entityId, expandedAttributeInstances as List<ExpandedTerm>, modifiedAt).bind()
                NGSILD_SCOPE_PROPERTY ->
                    scopeService.update(entityId, expandedAttributeInstances, modifiedAt, operationType).bind()
                else -> {
                    logger.warn("Ignoring unhandled core property: {}", expandedTerm)
                    FailedAttributeOperationResult(
                        attributeName = expandedTerm,
                        operationStatus = OperationStatus.FAILED,
                        errorMessage = "Ignoring unhandled core property: $expandedTerm"
                    ).right().bind()
                }
            }
        }
    }

    @Transactional
    suspend fun updateTypes(
        entityId: URI,
        newTypes: List<ExpandedTerm>,
        modifiedAt: ZonedDateTime,
        allowEmptyListOfTypes: Boolean = true
    ): Either<APIException, SucceededAttributeOperationResult> = either {
        val entityPayload = entityQueryService.retrieve(entityId).bind()
        val currentTypes = entityPayload.types
        // when dealing with an entity update, list of types can be empty if no change of type is requested
        if (currentTypes.sorted() == newTypes.sorted() || newTypes.isEmpty() && allowEmptyListOfTypes)
            return@either SucceededAttributeOperationResult(
                attributeName = JSONLD_TYPE,
                operationStatus = OperationStatus.CREATED,
                newExpandedValue = mapOf(JSONLD_TYPE to currentTypes.toList())
            )

        val updatedTypes = currentTypes.union(newTypes)
        val updatedPayload = entityPayload.payload.deserializeAsMap().plus(JSONLD_TYPE to updatedTypes)

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
            .bind("modified_at", modifiedAt)
            .bind("types", updatedTypes.toTypedArray())
            .bind("payload", Json.of(serializeObject(updatedPayload)))
            .execute()
            .map {
                SucceededAttributeOperationResult(
                    attributeName = JSONLD_TYPE,
                    operationStatus = OperationStatus.CREATED,
                    newExpandedValue = mapOf(JSONLD_TYPE to updatedTypes.toList())
                )
            }.bind()
    }

    @Transactional
    suspend fun appendAttributes(
        entityId: URI,
        expandedAttributes: ExpandedAttributes,
        disallowOverwrite: Boolean,
        sub: Sub? = null
    ): Either<APIException, UpdateResult> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()

        val (coreAttrs, otherAttrs) =
            expandedAttributes.toList().partition { JSONLD_EXPANDED_ENTITY_SPECIFIC_MEMBERS.contains(it.first) }
        val createdAt = ngsiLdDateTime()

        val operationType =
            if (disallowOverwrite) APPEND_ATTRIBUTES
            else APPEND_ATTRIBUTES_OVERWRITE_ALLOWED
        val coreOperationResult = updateCoreAttributes(entityId, coreAttrs, createdAt, operationType).bind()
        val attrsOperationResult = entityAttributeService.appendAttributes(
            entityId,
            otherAttrs.toMap().toNgsiLdAttributes().bind(),
            expandedAttributes,
            disallowOverwrite,
            createdAt,
            sub
        ).bind()

        val operationResult = coreOperationResult.plus(attrsOperationResult)
        handleSuccessOperationActions(operationResult, entityId, createdAt, sub).bind()

        UpdateResult(operationResult)
    }

    @Transactional
    suspend fun updateAttributes(
        entityId: URI,
        expandedAttributes: ExpandedAttributes,
        sub: Sub? = null
    ): Either<APIException, UpdateResult> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()

        val (coreAttrs, otherAttrs) =
            expandedAttributes.toList().partition { JSONLD_EXPANDED_ENTITY_SPECIFIC_MEMBERS.contains(it.first) }
        val createdAt = ngsiLdDateTime()

        val coreOperationResult = updateCoreAttributes(entityId, coreAttrs, createdAt, UPDATE_ATTRIBUTES).bind()
        val attrsOperationResult = entityAttributeService.updateAttributes(
            entityId,
            otherAttrs.toMap().toNgsiLdAttributes().bind(),
            expandedAttributes,
            createdAt,
            sub
        ).bind()

        val operationResult = coreOperationResult.plus(attrsOperationResult)
        handleSuccessOperationActions(operationResult, entityId, createdAt, sub).bind()

        UpdateResult(operationResult)
    }

    @Transactional
    suspend fun partialUpdateAttribute(
        entityId: URI,
        expandedAttribute: ExpandedAttribute,
        sub: Sub? = null
    ): Either<APIException, UpdateResult> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()

        val modifiedAt = ngsiLdDateTime()

        val operationResult = entityAttributeService.partialUpdateAttribute(
            entityId,
            expandedAttribute,
            modifiedAt,
            sub
        ).bind().let { listOf(it) }

        handleSuccessOperationActions(operationResult, entityId, modifiedAt, sub).bind()

        UpdateResult(operationResult)
    }

    @Transactional
    suspend fun upsertAttributes(
        entityId: URI,
        expandedAttributes: ExpandedAttributes,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        val createdAt = ngsiLdDateTime()
        expandedAttributes.forEach { (attributeName, expandedAttributeInstances) ->
            expandedAttributeInstances.forEach { expandedAttributeInstance ->
                val jsonLdAttribute = mapOf(attributeName to listOf(expandedAttributeInstance))
                val ngsiLdAttribute = jsonLdAttribute.toNgsiLdAttributes().bind()[0]

                entityAttributeService.upsertAttributes(
                    entityId,
                    ngsiLdAttribute,
                    jsonLdAttribute,
                    createdAt,
                    sub
                ).bind()
            }
        }
        updateState(
            entityId,
            createdAt,
            entityAttributeService.getForEntity(entityId, emptySet(), emptySet())
        ).bind()
    }

    @Transactional
    suspend fun replaceAttribute(
        entityId: URI,
        expandedAttribute: ExpandedAttribute,
        sub: Sub? = null
    ): Either<APIException, UpdateResult> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()

        val ngsiLdAttribute = listOf(expandedAttribute).toMap().toNgsiLdAttributes().bind()[0]
        val replacedAt = ngsiLdDateTime()

        val operationResult = entityAttributeService.replaceAttribute(
            entityId,
            ngsiLdAttribute,
            expandedAttribute,
            replacedAt,
            sub
        ).bind().let { listOf(it) }

        handleSuccessOperationActions(operationResult, entityId, replacedAt, sub).bind()

        UpdateResult(operationResult)
    }

    @Transactional
    internal suspend fun handleSuccessOperationActions(
        operationResult: List<AttributeOperationResult>,
        entityId: URI,
        createdAt: ZonedDateTime,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        // update modifiedAt in entity if at least one attribute has been added
        if (operationResult.hasSuccessfulResult()) {
            val attributes = entityAttributeService.getForEntity(entityId, emptySet(), emptySet())
            updateState(entityId, createdAt, attributes).bind()

            entityEventService.publishAttributeChangeEvents(
                sub,
                entityId,
                operationResult.getSucceededOperations()
            )
        }
    }

    @Transactional
    suspend fun updateState(
        entityUri: URI,
        modifiedAt: ZonedDateTime,
        attributes: List<Attribute>
    ): Either<APIException, Unit> =
        entityQueryService.retrieve(entityUri)
            .map { entityPayload ->
                val payload = buildJsonLdEntity(
                    attributes,
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
        attributes: List<Attribute>,
        entity: Entity
    ): Map<String, Any> {
        val entityCoreAttributes = entity.serializeProperties()
        val expandedAttributes = attributes
            .groupBy { attribute ->
                attribute.attributeName
            }
            .mapValues { (_, attributes) ->
                attributes.map { attribute ->
                    attribute.payload.deserializeExpandedPayload()
                        .addSysAttrs(withSysAttrs = true, attribute.createdAt, attribute.modifiedAt)
                }
            }

        return entityCoreAttributes.plus(expandedAttributes)
    }

    @Transactional
    suspend fun deleteEntity(entityId: URI, sub: Sub? = null): Either<APIException, Unit> = either {
        val currentEntity = entityQueryService.retrieve(entityId).bind()
        authorizationService.userCanAdminEntity(entityId, sub.toOption()).bind()

        val deletedAt = ngsiLdDateTime()
        val deletedEntityPayload = currentEntity.toExpandedDeletedEntity(deletedAt)
        val previousEntity = deleteEntityPayload(entityId, deletedAt, deletedEntityPayload).bind()
        val deleteOperationResult = entityAttributeService.deleteAttributes(entityId, deletedAt).bind()
        scopeService.addHistoryEntry(entityId, emptyList(), TemporalProperty.DELETED_AT, deletedAt, sub).bind()

        entityEventService.publishEntityDeleteEvent(sub, previousEntity, deletedEntityPayload)
        entityEventService.publishAttributeChangeEvents(
            sub,
            entityId,
            deleteOperationResult.getSucceededOperations()
        )
    }

    @Transactional
    suspend fun deleteEntityPayload(
        entityId: URI,
        deletedAt: ZonedDateTime,
        deletedEntityPayload: ExpandedEntity
    ): Either<APIException, Entity> = either {
        databaseClient.sql(
            """
            WITH entity_before_delete AS (
                SELECT *
                FROM entity_payload
                WHERE entity_id = :entity_id
            ),
            update_entity AS (
                UPDATE entity_payload
                SET deleted_at = :deleted_at,
                    payload = :payload
                WHERE entity_id = :entity_id
            )
            SELECT * FROM entity_before_delete
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("deleted_at", deletedAt)
            .bind("payload", Json.of(serializeObject(deletedEntityPayload.members)))
            .oneToResult {
                it.rowToEntity()
            }
            .bind()
    }

    @Transactional
    suspend fun permanentlyDeleteEntity(entityId: URI, sub: Sub? = null): Either<APIException, Unit> = either {
        val currentEntity = entityQueryService.retrieve(entityId, false).bind()
        authorizationService.userCanAdminEntity(entityId, sub.toOption()).bind()

        val previousEntity = permanentyDeleteEntityPayload(entityId).bind()
        entityAttributeService.permanentlyDeleteAttributes(entityId).bind()
        authorizationService.removeRightsOnEntity(entityId).bind()

        if (currentEntity.deletedAt == null) {
            // only send a notification if entity was not already previously deleted
            val deletedEntityPayload = currentEntity.toExpandedDeletedEntity(ngsiLdDateTime())
            entityEventService.publishEntityDeleteEvent(sub, previousEntity, deletedEntityPayload)
        }
    }

    @Transactional
    suspend fun permanentyDeleteEntityPayload(entityId: URI): Either<APIException, Entity> = either {
        databaseClient.sql(
            """
            DELETE FROM entity_payload
            WHERE entity_id = :entity_id
            RETURNING *
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .oneToResult {
                it.rowToEntity()
            }
            .bind()
    }

    @Transactional
    suspend fun deleteAttribute(
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI?,
        deleteAll: Boolean = false,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()

        val deleteAttributeResults = if (attributeName == NGSILD_SCOPE_PROPERTY) {
            scopeService.delete(entityId).bind()
        } else {
            entityAttributeService.checkEntityAndAttributeExistence(
                entityId,
                attributeName,
                datasetId,
                deleteAll
            ).bind()
            entityAttributeService.deleteAttribute(
                entityId,
                attributeName,
                datasetId,
                deleteAll,
                ngsiLdDateTime()
            ).bind()
        }
        updateState(
            entityId,
            ngsiLdDateTime(),
            entityAttributeService.getForEntity(entityId, emptySet(), emptySet())
        ).bind()

        deleteAttributeResults.filterIsInstance<SucceededAttributeOperationResult>()
            .forEach {
                entityEventService.publishAttributeDeleteEvent(sub, entityId, it)
            }
    }

    @Transactional
    suspend fun permanentlyDeleteAttribute(
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI?,
        deleteAll: Boolean = false,
        sub: Sub? = null
    ): Either<APIException, Unit> = either {
        authorizationService.userCanUpdateEntity(entityId, sub.toOption()).bind()

        if (attributeName == NGSILD_SCOPE_PROPERTY) {
            scopeService.permanentlyDelete(entityId).bind()
        } else {
            entityAttributeService.checkEntityAndAttributeExistence(
                entityId,
                attributeName,
                datasetId
            ).bind()
            entityAttributeService.permanentlyDeleteAttribute(
                entityId,
                attributeName,
                datasetId,
                deleteAll
            ).bind()
        }
        updateState(
            entityId,
            ngsiLdDateTime(),
            entityAttributeService.getForEntity(entityId, emptySet(), emptySet())
        ).bind()
    }
}
