package com.egm.stellio.search.entity.service

import arrow.core.Either
import arrow.core.getOrElse
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.getSpecificAccessPolicy
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.common.util.DiagnosticTimers
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.executeExpected
import com.egm.stellio.search.common.util.oneToResult
import com.egm.stellio.search.common.util.toList
import com.egm.stellio.search.common.util.toZonedDateTime
import com.egm.stellio.search.entity.model.AttributeOperationResult
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
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
import com.egm.stellio.search.entity.web.BatchEntityError
import com.egm.stellio.search.entity.web.BatchOperationResult
import com.egm.stellio.search.scope.ScopeService
import com.egm.stellio.search.temporal.model.AttributeInstance.TemporalProperty
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.EXPANDED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.model.EXPANDED_ENTITY_SPECIFIC_MEMBERS
import com.egm.stellio.shared.model.ExpandedAttribute
import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.model.ExpandedAttributeInstances
import com.egm.stellio.shared.model.ExpandedAttributes
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_NONE_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_IRI
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_SCOPE_IRI
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.flattenOnAttributeAndDatasetId
import com.egm.stellio.shared.model.getDatasetId
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.model.toNgsiLdAttributes
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.ErrorMessages.DbQuery.OPERATION_NO_RESULT_MESSAGE
import com.egm.stellio.shared.util.ErrorMessages.Entity.entityAlreadyExistsMessage
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.wrapToList
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
        expandedEntity: ExpandedEntity
    ): Either<APIException, Unit> = kotlin.runCatching {
        either<APIException, Unit> {
            val sub = getSubFromSecurityContext()

            val (neverExisted, markedDeleted) = entityQueryService.isMarkedAsDeleted(ngsiLdEntity.id).let {
                it.isLeft() to it.getOrElse { false }
            }

            when {
                neverExisted -> authorizationService.userCanCreateEntities().bind()
                markedDeleted ->
                    authorizationService.userCanAdminEntity(ngsiLdEntity.id).bind()
                !markedDeleted ->
                    AlreadyExistsException(entityAlreadyExistsMessage(ngsiLdEntity.id)).left().bind()
            }

            val createdAt = ngsiLdDateTime()
            val expandedEntityWithMetadata = expandedEntity.populateCreationTimeDate(createdAt)
            val attributesMetadata = ngsiLdEntity.prepareAttributes().bind()
            logger.debug("Creating entity {}", ngsiLdEntity.id)

            createEntityPayload(ngsiLdEntity, expandedEntityWithMetadata, createdAt).bind()
            scopeService.create(ngsiLdEntity, createdAt).bind()
            val attrsOperationResult = entityAttributeService.createAttributes(
                ngsiLdEntity,
                expandedEntity,
                attributesMetadata,
                createdAt
            ).bind()

            ngsiLdEntity.getSpecificAccessPolicy()?.bind()
                ?.let { specificAccessPolicy ->
                    authorizationService.createGlobalPermission(
                        ngsiLdEntity.id,
                        action = specificAccessPolicy
                    )
                }

            if (neverExisted)
                authorizationService.createEntityOwnerRight(ngsiLdEntity.id).bind()

            entityEventService.publishEntityCreateEvent(
                sub,
                expandedEntity
            )
            entityEventService.publishAttributeChangeEvents(
                sub,
                ExpandedEntity(emptyMap()),
                expandedEntityWithMetadata,
                attrsOperationResult.getSucceededOperations()
            )
        }
    }.fold(
        onFailure = { it.toAPIException().left() },
        onSuccess = { it }
    )

    @Transactional
    suspend fun createEntityPayload(
        ngsiLdEntity: NgsiLdEntity,
        expandedEntity: ExpandedEntity,
        createdAt: ZonedDateTime
    ): Either<APIException, Unit> = either {
        databaseClient.sql(
            """
            INSERT INTO entity_payload
                (entity_id, types, scopes, created_at, modified_at, expires_at, payload)
            VALUES
                (:entity_id, :types, :scopes, :created_at, :created_at, :expires_at, :payload)
            ON CONFLICT (entity_id)
                DO UPDATE SET types = :types,
                    scopes = :scopes,
                    modified_at = :created_at,
                    deleted_at = null,
                    expires_at = :expires_at,
                    payload = :payload
            """.trimIndent()
        )
            .bind("entity_id", ngsiLdEntity.id)
            .bind("types", ngsiLdEntity.types.toTypedArray())
            .bind("scopes", ngsiLdEntity.scopes?.toTypedArray())
            .bind("created_at", createdAt)
            .bind("expires_at", ngsiLdEntity.expiresAt)
            .bind("payload", Json.of(serializeObject(expandedEntity.members)))
            .execute()
    }

    @Transactional
    suspend fun mergeEntity(
        entityId: URI,
        expandedAttributes: ExpandedAttributes,
        observedAt: ZonedDateTime?
    ): Either<APIException, UpdateResult> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId).bind()

        val mergedAt = ngsiLdDateTime()
        val originalEntity = entityQueryService.retrieve(entityId).bind().toExpandedEntity()
        val (coreAttrs, otherAttrs) =
            expandedAttributes.toList()
                // remove @id if it is present (optional as per 5.4)
                .filter { it.first != JSONLD_ID_KW }
                .partition { EXPANDED_ENTITY_SPECIFIC_MEMBERS.contains(it.first) }
        logger.debug("Merging entity {}", entityId)

        val coreOperationResult = updateCoreAttributes(entityId, coreAttrs, mergedAt, MERGE_ENTITY).bind()
        val attrsOperationResult = entityAttributeService.mergeAttributes(
            entityId,
            otherAttrs.toMap().toNgsiLdAttributes().bind(),
            expandedAttributes,
            mergedAt,
            observedAt
        ).bind()

        val operationResult = coreOperationResult.plus(attrsOperationResult)
        handleSuccessOperationActions(entityId, originalEntity, operationResult, mergedAt).bind()

        UpdateResult(operationResult)
    }

    @Transactional
    suspend fun replaceEntity(
        entityId: URI,
        ngsiLdEntity: NgsiLdEntity,
        expandedEntity: ExpandedEntity
    ): Either<APIException, UpdateResult> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId).bind()

        logger.debug("Replacing entity {}", ngsiLdEntity.id)

        val replacedAt = ngsiLdDateTime()
        val originalEntity = entityQueryService.retrieve(entityId).bind().toExpandedEntity()
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
                    replacedAt
                ).bind()
            }

        replaceEntityPayload(ngsiLdEntity, expandedEntity, replacedAt).bind()
        scopeService.replace(ngsiLdEntity, replacedAt).bind()

        val operationResult = deleteOperationResults.plus(createOrReplaceOperationResult)
        operationResult.getSucceededOperations()
            .forEach {
                val sub = getSubFromSecurityContext()
                if (it.operationStatus == OperationStatus.DELETED)
                    entityEventService.publishAttributeDeleteEvent(sub, originalEntity, expandedEntity, it)
                else
                    entityEventService.publishAttributeChangeEvents(
                        sub,
                        originalEntity,
                        expandedEntity,
                        listOf(it)
                    )
            }

        UpdateResult(operationResult)
    }

    @Transactional
    suspend fun replaceEntityPayload(
        ngsiLdEntity: NgsiLdEntity,
        expandedEntity: ExpandedEntity,
        replacedAt: ZonedDateTime
    ): Either<APIException, Unit> = either {
        val createdAt = retrieveCreatedAt(ngsiLdEntity.id).bind()
        val serializedPayload =
            serializeObject(expandedEntity.populateReplacementTimeDates(createdAt, replacedAt).members)

        databaseClient.sql(
            """
            UPDATE entity_payload
            SET types = :types,
                scopes = :scopes,
                modified_at = :modified_at,
                expires_at = :expires_at,
                payload = :payload
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", ngsiLdEntity.id)
            .bind("types", ngsiLdEntity.types.toTypedArray())
            .bind("scopes", ngsiLdEntity.scopes?.toTypedArray())
            .bind("modified_at", replacedAt)
            .bind("expires_at", ngsiLdEntity.expiresAt)
            .bind("payload", Json.of(serializedPayload))
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
                JSONLD_TYPE_KW ->
                    updateTypes(entityId, expandedAttributeInstances as List<ExpandedTerm>, modifiedAt).bind()
                NGSILD_SCOPE_IRI ->
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
        modifiedAt: ZonedDateTime
    ): Either<APIException, SucceededAttributeOperationResult> = either {
        databaseClient.sql(
            """
            WITH new_types AS (
                SELECT COALESCE(
                    (
                        SELECT array_agg(new_type ORDER BY ordinality)
                        FROM unnest(:new_types::text[]) WITH ORDINALITY AS u(new_type, ordinality)
                        WHERE new_type <> ALL(ep.types)
                    ),
                    '{}'::text[]
                ) AS to_add
                FROM entity_payload ep
                WHERE ep.entity_id = :entity_id
            )
            UPDATE entity_payload
            SET types = entity_payload.types || new_types.to_add,
                modified_at = :modified_at,
                payload = entity_payload.payload || jsonb_build_object(
                    '@type',
                    to_jsonb(entity_payload.types || new_types.to_add)
                )
            FROM new_types
            WHERE entity_payload.entity_id = :entity_id
            RETURNING types
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("modified_at", modifiedAt)
            .bind("new_types", newTypes.toTypedArray())
            .oneToResult { row ->
                SucceededAttributeOperationResult(
                    attributeName = JSONLD_TYPE_KW,
                    operationStatus = OperationStatus.CREATED,
                    newExpandedValue = mapOf(JSONLD_TYPE_KW to toList<ExpandedTerm>(row["types"]))
                )
            }.bind()
    }

    @Transactional
    suspend fun appendAttributes(
        entityId: URI,
        expandedAttributes: ExpandedAttributes,
        disallowOverwrite: Boolean
    ): Either<APIException, UpdateResult> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId).bind()

        val createdAt = ngsiLdDateTime()
        val originalEntity = entityQueryService.retrieve(entityId).bind().toExpandedEntity()
        val (coreAttrs, otherAttrs) =
            expandedAttributes.toList().partition { EXPANDED_ENTITY_SPECIFIC_MEMBERS.contains(it.first) }

        val operationType =
            if (disallowOverwrite) APPEND_ATTRIBUTES
            else APPEND_ATTRIBUTES_OVERWRITE_ALLOWED
        val coreOperationResult = updateCoreAttributes(entityId, coreAttrs, createdAt, operationType).bind()
        val attrsOperationResult = entityAttributeService.appendAttributes(
            entityId,
            otherAttrs.toMap().toNgsiLdAttributes().bind(),
            expandedAttributes,
            disallowOverwrite,
            createdAt
        ).bind()

        val operationResult = coreOperationResult.plus(attrsOperationResult)
        handleSuccessOperationActions(entityId, originalEntity, operationResult, createdAt).bind()

        UpdateResult(operationResult)
    }

    @Transactional
    suspend fun updateAttributes(
        entityId: URI,
        expandedAttributes: ExpandedAttributes
    ): Either<APIException, UpdateResult> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId).bind()

        val createdAt = ngsiLdDateTime()
        val originalEntity = entityQueryService.retrieve(entityId).bind().toExpandedEntity()
        val (coreAttrs, otherAttrs) =
            expandedAttributes.toList().partition { EXPANDED_ENTITY_SPECIFIC_MEMBERS.contains(it.first) }

        val coreOperationResult = updateCoreAttributes(entityId, coreAttrs, createdAt, UPDATE_ATTRIBUTES).bind()
        val attrsOperationResult = entityAttributeService.updateAttributes(
            entityId,
            otherAttrs.toMap().toNgsiLdAttributes().bind(),
            expandedAttributes,
            createdAt
        ).bind()

        val operationResult = coreOperationResult.plus(attrsOperationResult)
        handleSuccessOperationActions(entityId, originalEntity, operationResult, createdAt).bind()

        UpdateResult(operationResult)
    }

    @Transactional
    suspend fun partialUpdateAttribute(
        entityId: URI,
        expandedAttribute: ExpandedAttribute,
        dispatchStartNanos: Long? = null
    ): Either<APIException, UpdateResult> = either {
        // Diagnostic-only: brackets AOP proxy dispatch + transaction BEGIN, neither of which any
        // internal timer can reach - dispatchStartNanos is captured by the caller before this
        // transactional method is even entered.
        dispatchStartNanos?.let {
            DiagnosticTimers.record("partialAttributeUpdate.dispatchInAndBegin", System.nanoTime() - it)
        }
        DiagnosticTimers.timeCommit("partialAttributeUpdate.commit")
        DiagnosticTimers.time("partialAttributeUpdate.checkEntityExistence") {
            entityQueryService.checkEntityExistence(entityId)
        }.bind()
        DiagnosticTimers.time("partialAttributeUpdate.userCanUpdateEntity") {
            authorizationService.userCanUpdateEntity(entityId)
        }.bind()

        val modifiedAt = ngsiLdDateTime()
        val originalEntity = DiagnosticTimers.time("partialAttributeUpdate.retrieve") {
            entityQueryService.retrieve(entityId)
        }.bind().toExpandedEntity()

        val operationResult = DiagnosticTimers.time("partialAttributeUpdate.entityAttributeService") {
            entityAttributeService.partialUpdateAttribute(
                entityId,
                expandedAttribute,
                modifiedAt
            )
        }.bind().wrapToList()

        // handleSuccessOperationActions(entityId, originalEntity, operationResult, modifiedAt).bind()

        UpdateResult(operationResult)
    }

    @Transactional
    suspend fun mergeAttribute(
        entityId: URI,
        expandedAttributeName: ExpandedTerm,
        attributeFragment: ExpandedAttributeInstance,
        observedAt: ZonedDateTime
    ): Either<APIException, List<SucceededAttributeOperationResult>> = either {
        val mergedAt = ngsiLdDateTime()
        val originalEntity = entityQueryService.retrieve(entityId).bind().toExpandedEntity()
        val expandedAttributes: ExpandedAttributes = mapOf(expandedAttributeName to listOf(attributeFragment))
        val ngsiLdAttributes = expandedAttributes.toNgsiLdAttributes().bind()

        val operationResult = entityAttributeService.mergeAttributes(
            entityId,
            ngsiLdAttributes,
            expandedAttributes,
            mergedAt,
            observedAt
        ).bind()

        if (operationResult.isNotEmpty()) {
            val updatedEntity = patchEntityPayload(entityId, originalEntity, mergedAt, operationResult).bind()
            entityEventService.publishAttributeChangeEvents(null, originalEntity, updatedEntity, operationResult)
        }

        operationResult
    }

    @Transactional
    suspend fun upsertAttributes(
        entityId: URI,
        expandedAttributes: ExpandedAttributes
    ): Either<APIException, Unit> = either {
        val createdAt = ngsiLdDateTime()

        val operationResults = expandedAttributes.flatMap { (attributeName, expandedAttributeInstances) ->
            expandedAttributeInstances.map { expandedAttributeInstance ->
                val jsonLdAttribute = mapOf(attributeName to listOf(expandedAttributeInstance))
                val ngsiLdAttribute = jsonLdAttribute.toNgsiLdAttributes().bind()[0]

                entityAttributeService.upsertAttributes(
                    entityId,
                    ngsiLdAttribute,
                    jsonLdAttribute,
                    createdAt
                ).bind()
            }
        }.filterNotNull()

        if (operationResults.isNotEmpty()) {
            patchEntityPayload(entityId, createdAt, operationResults).bind()
        } else {
            // only added history entries, no modification of current state of attributes
            updateEntityModifiedAt(entityId, createdAt).bind()
        }
    }

    @Transactional
    suspend fun replaceAttribute(
        entityId: URI,
        expandedAttribute: ExpandedAttribute
    ): Either<APIException, UpdateResult> = either {
        entityQueryService.checkEntityExistence(entityId).bind()
        authorizationService.userCanUpdateEntity(entityId).bind()

        val replacedAt = ngsiLdDateTime()
        val originalEntity = entityQueryService.retrieve(entityId).bind().toExpandedEntity()
        val ngsiLdAttribute = listOf(expandedAttribute).toMap().toNgsiLdAttributes().bind()[0]

        val operationResult = entityAttributeService.replaceAttribute(
            entityId,
            ngsiLdAttribute,
            expandedAttribute,
            replacedAt
        ).bind().wrapToList()

        handleSuccessOperationActions(entityId, originalEntity, operationResult, replacedAt).bind()

        UpdateResult(operationResult)
    }

    @Transactional
    internal suspend fun handleSuccessOperationActions(
        entityId: URI,
        originalEntity: ExpandedEntity,
        operationResult: List<AttributeOperationResult>,
        createdAt: ZonedDateTime
    ): Either<APIException, Unit> = either {
        if (operationResult.hasSuccessfulResult()) {
            val sub = getSubFromSecurityContext()
            val updatedEntity = patchEntityPayload(
                entityId,
                originalEntity,
                createdAt,
                operationResult.getSucceededOperations(false)
            ).bind()

            entityEventService.publishAttributeChangeEvents(
                sub,
                originalEntity,
                updatedEntity,
                operationResult.getSucceededOperations()
            )
        }
    }

    @Transactional
    suspend fun deleteEntity(
        entityId: URI,
        inTransientPurge: Boolean = false
    ): Either<APIException, Unit> = runCatching {
        either<APIException, Unit> {
            val sub = getSubFromSecurityContext()
            val currentEntity = entityQueryService.retrieve(entityId).bind()
            if (!inTransientPurge)
                authorizationService.userCanAdminEntity(entityId).bind()

            val deletedAt = ngsiLdDateTime()
            val deletedEntityPayload = currentEntity.toExpandedDeletedEntity(deletedAt)
            val previousEntity = deleteEntityPayload(entityId, deletedAt, deletedEntityPayload).bind()
            val deleteOperationResult = entityAttributeService.deleteAttributes(entityId, deletedAt).bind()
            if (!previousEntity.scopes.isNullOrEmpty())
                scopeService.addHistoryEntry(entityId, emptyList(), TemporalProperty.DELETED_AT, deletedAt).bind()

            entityEventService.publishAttributeDeletesOnEntityDeleteEvent(
                sub,
                currentEntity.toExpandedEntity(),
                deletedEntityPayload,
                deleteOperationResult.getSucceededOperations()
            )
            entityEventService.publishEntityDeleteEvent(sub, previousEntity, deletedEntityPayload)
        }
    }.fold(
        onFailure = { it.toAPIException().left() },
        onSuccess = { it }
    )

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
    suspend fun permanentlyDeleteEntity(
        entityId: URI,
        inUserDeletion: Boolean = false
    ): Either<APIException, Unit> = either {
        val sub = getSubFromSecurityContext()
        val currentEntity = entityQueryService.retrieve(entityId, false).bind()
        if (!inUserDeletion)
            authorizationService.userCanAdminEntity(entityId).bind()

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
        inTransientPurge: Boolean = false
    ): Either<APIException, Unit> = either {
        val sub = getSubFromSecurityContext()
        if (!inTransientPurge)
            authorizationService.userCanUpdateEntity(entityId).bind()

        val originalEntity = entityQueryService.retrieve(entityId).bind().toExpandedEntity()

        val deletedAt = ngsiLdDateTime()
        val deleteAttributeResults = if (attributeName == NGSILD_SCOPE_IRI) {
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
                deletedAt
            ).bind()
        }
        val updatedEntity = patchEntityPayload(
            entityId,
            originalEntity,
            deletedAt,
            deleteAttributeResults.getSucceededOperations(false)
        ).bind()

        deleteAttributeResults.getSucceededOperations()
            .forEach {
                entityEventService.publishAttributeDeleteEvent(sub, originalEntity, updatedEntity, it)
            }
    }

    @Transactional
    suspend fun purgeEntities(
        entitiesQuery: EntitiesQueryFromGet,
        keep: Set<ExpandedTerm> = emptySet(),
        drop: Set<ExpandedTerm> = emptySet()
    ): Either<APIException, BatchOperationResult> = either {
        val matchingIds = entityQueryService.queryEntities(
            entitiesQuery.copy(
                // pass a special value of -1 to say we want all the results
                // this is later handled in the queryEntities function
                paginationQuery = PaginationQuery(0, -1)
            ),
            excludeDeleted = true,
            authorizationService.getAccessRightWithClauseAndFilter(Action.ADMIN)
        )

        if (matchingIds.isEmpty())
            return BatchOperationResult().right()

        val result = BatchOperationResult()

        matchingIds.forEach { entityId ->
            when {
                keep.isEmpty() && drop.isEmpty() ->
                    deleteEntity(entityId).fold(
                        { result.errors.add(BatchEntityError(entityId, it.toProblemDetail())) },
                        { result.success.add(entityId) }
                    )
                keep.isNotEmpty() -> {
                    val currentAttrNames = entityAttributeService
                        .getAllForEntity(entityId, excludeDeleted = true)
                        .map { it.attributeName }
                        .toSet()
                    currentAttrNames.minus(keep).forEach { attrName ->
                        deleteAttribute(entityId, attrName, null, true).fold(
                            { result.errors.add(BatchEntityError(entityId, it.toProblemDetail())) },
                            { result.success.add(entityId) }
                        )
                    }
                }
                else ->
                    drop.forEach { attrName ->
                        deleteAttribute(entityId, attrName, null, true).fold(
                            { result.errors.add(BatchEntityError(entityId, it.toProblemDetail())) },
                            { result.success.add(entityId) }
                        )
                    }
            }
        }

        result
    }

    /**
     * NGSI-LD 4.22 - Transient Storage of Entities and Attributes: garbage-collects entities and attributes whose
     * expiresAt lies in the past. Called periodically by TransientDataGarbageCollectionJob, outside of any user
     * request, so authorization checks are bypassed (there is no requesting user to authorize against).
     */
    @Transactional
    suspend fun purgeExpiredEntitiesAndAttributes(): BatchOperationResult {
        val result = BatchOperationResult()

        val expiredEntitiesIds = entityQueryService.getExpiredEntitiesIds()
        expiredEntitiesIds.forEach { entityId ->
            deleteEntity(entityId, inTransientPurge = true).fold(
                { result.errors.add(BatchEntityError(entityId, it.toProblemDetail())) },
                { result.success.add(entityId) }
            )
        }

        entityAttributeService.getExpiredAttributes()
            .filter { it.entityId !in expiredEntitiesIds }
            .forEach { attribute ->
                deleteAttribute(
                    attribute.entityId,
                    attribute.attributeName,
                    attribute.datasetId,
                    deleteAll = false,
                    inTransientPurge = true
                ).fold(
                    { result.errors.add(BatchEntityError(attribute.entityId, it.toProblemDetail())) },
                    { result.success.add(attribute.entityId) }
                )
            }

        return result
    }

    @Transactional
    suspend fun permanentlyDeleteAttribute(
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI?,
        deleteAll: Boolean = false
    ): Either<APIException, Unit> = either {
        authorizationService.userCanUpdateEntity(entityId).bind()
        val modifiedAt = ngsiLdDateTime()

        if (attributeName == NGSILD_SCOPE_IRI) {
            scopeService.permanentlyDelete(entityId, modifiedAt).bind()
        } else {
            entityAttributeService.checkEntityAndAttributeExistence(
                entityId,
                attributeName,
                datasetId,
                anyAttributeInstance = deleteAll,
                excludeDeleted = false
            ).bind()
            if (deleteAll) {
                entityAttributeService.permanentlyDeleteAttribute(entityId, attributeName, datasetId, true).bind()
                removeAttributeFromPayload(entityId, modifiedAt, attributeName).bind()
            } else {
                entityAttributeService.permanentlyDeleteAttribute(entityId, attributeName, datasetId, false).bind()
                patchEntityPayload(
                    entityId,
                    modifiedAt,
                    listOf(
                        SucceededAttributeOperationResult(
                            attributeName = attributeName,
                            datasetId = datasetId,
                            operationStatus = OperationStatus.DELETED,
                            newExpandedValue = emptyMap()
                        )
                    )
                ).bind()
            }
        }
    }

    // Persists all attribute changes of one operation into entity_payload, delegating to
    // mergeAttributeIntoPayload once per distinct attribute name (or just bumping modifiedAt if no
    // attribute actually changed). Does not return the resulting entity - for callers that don't
    // need to build a Kafka event from it.
    private suspend fun patchEntityPayload(
        entityId: URI,
        modifiedAt: ZonedDateTime,
        operationResults: List<SucceededAttributeOperationResult>
    ): Either<APIException, Unit> = either {
        val modifiedAtPatch = Json.of(
            serializeObject(mapOf(NGSILD_MODIFIED_AT_IRI to buildNonReifiedTemporalValue(modifiedAt)))
        )
        val attributeGroups = operationResults.groupBy { it.attributeName }

        if (attributeGroups.isEmpty()) {
            databaseClient.sql(
                """
                UPDATE entity_payload
                SET modified_at = :modified_at,
                    payload = (payload || :modified_at_patch::jsonb)
                WHERE entity_id = :entity_id
                """.trimIndent()
            )
                .bind("entity_id", entityId)
                .bind("modified_at", modifiedAt)
                .bind("modified_at_patch", modifiedAtPatch)
                .executeExpected { notFoundIfNoRowsUpdated(it) }
                .bind()
        } else {
            attributeGroups.forEach { (attrName, results) ->
                mergeAttributeIntoPayload(entityId, attrName, results, modifiedAt, modifiedAtPatch).bind()
            }
        }
    }

    // Same as the overload above, but also returns the resulting entity, computed in memory instead
    // of re-reading it from the database. For callers that publish a Kafka event from the result.
    private suspend fun patchEntityPayload(
        entityId: URI,
        originalEntity: ExpandedEntity,
        modifiedAt: ZonedDateTime,
        operationResults: List<SucceededAttributeOperationResult>
    ): Either<APIException, ExpandedEntity> = either {
        val attributeResults = operationResults.filter { it.attributeName !in EXPANDED_ENTITY_CORE_MEMBERS }
        patchEntityPayload(entityId, modifiedAt, attributeResults).bind()
        originalEntity.mergeSucceededOperationResults(operationResults, modifiedAt)
    }

    // The per-attribute unit of work patchEntityPayload delegates to: keeps this attribute's
    // untouched instances, drops the touched ones, and appends the new/updated ones.
    private suspend fun mergeAttributeIntoPayload(
        entityId: URI,
        attrName: ExpandedTerm,
        results: List<SucceededAttributeOperationResult>,
        modifiedAt: ZonedDateTime,
        modifiedAtPatch: Json
    ): Either<APIException, Unit> {
        val deletedDatasetIds = results
            .filter { it.operationStatus == OperationStatus.DELETED }
            .map { it.datasetId }
        val upsertedResults = results.filter { it.operationStatus != OperationStatus.DELETED }
        // datasetIds touched by this operation (deleted or replaced): the corresponding
        // existing instances must not be kept from the live payload
        val excludedDatasetIds = (deletedDatasetIds + upsertedResults.map { it.datasetId })
            .map { it?.toString() ?: JSONLD_NONE_KW }
        val newInstances = upsertedResults.map { it.newExpandedValue }

        return databaseClient.sql(
            """
            UPDATE entity_payload
            SET modified_at = :modified_at,
                payload = (
                    (
                        SELECT CASE
                            WHEN kept.instances || :new_instances::jsonb = '[]'::jsonb
                            THEN payload - :attr_name::text
                            ELSE payload || jsonb_build_object(
                                :attr_name::text, kept.instances || :new_instances::jsonb
                            )
                        END
                        FROM (
                            SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb) AS instances
                            FROM jsonb_array_elements(
                                COALESCE(payload -> :attr_name::text, '[]'::jsonb)
                            ) AS elem
                            WHERE COALESCE(
                                elem #>> '{$NGSILD_DATASET_ID_IRI,0,$JSONLD_ID_KW}', '$JSONLD_NONE_KW'
                            ) <> ALL (:excluded_dataset_ids::text[])
                        ) AS kept
                    ) || :modified_at_patch::jsonb
                )
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("modified_at", modifiedAt)
            .bind("attr_name", attrName)
            .bind("excluded_dataset_ids", excludedDatasetIds.toTypedArray())
            .bind("new_instances", Json.of(serializeObject(newInstances)))
            .bind("modified_at_patch", modifiedAtPatch)
            .executeExpected { notFoundIfNoRowsUpdated(it) }
    }

    // Removes an attribute entirely from payload. Only needed for permanent deletion: soft delete
    // already produces per-instance DELETED results that mergeAttributeIntoPayload consumes and
    // drops the key from as a side effect, but permanent deletion produces no results to reuse
    // that path with.
    private suspend fun removeAttributeFromPayload(
        entityId: URI,
        modifiedAt: ZonedDateTime,
        attributeName: ExpandedTerm
    ): Either<APIException, Unit> = either {
        databaseClient.sql(
            """
            UPDATE entity_payload
            SET modified_at = :modified_at,
                payload = payload - :attribute_name::text
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("modified_at", modifiedAt)
            .bind("attribute_name", attributeName)
            .executeExpected { notFoundIfNoRowsUpdated(it) }
            .bind()
    }

    private suspend fun updateEntityModifiedAt(
        entityId: URI,
        modifiedAt: ZonedDateTime
    ): Either<APIException, Unit> {
        val patch = mapOf(NGSILD_MODIFIED_AT_IRI to buildNonReifiedTemporalValue(modifiedAt))
        return databaseClient.sql(
            """
            UPDATE entity_payload
            SET modified_at = :modified_at,
                payload = (payload || :patch::jsonb)
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("modified_at", modifiedAt)
            .bind("patch", Json.of(serializeObject(patch)))
            .executeExpected { notFoundIfNoRowsUpdated(it) }
    }

    private fun notFoundIfNoRowsUpdated(rowsUpdated: Long): Either<APIException, Unit> =
        if (rowsUpdated == 0L) ResourceNotFoundException(OPERATION_NO_RESULT_MESSAGE).left() else Unit.right()
}

// Mirrors the SQL merge performed by `EntityService.mergeAttributeIntoPayload` (and the plain
// modifiedAt-only patch in `EntityService.patchEntityPayload`) purely in memory.
internal fun ExpandedEntity.mergeSucceededOperationResults(
    operationResults: List<SucceededAttributeOperationResult>,
    modifiedAt: ZonedDateTime
): ExpandedEntity {
    val (coreResults, attributeResults) =
        operationResults.partition { it.attributeName in EXPANDED_ENTITY_CORE_MEMBERS }

    // core members (@type, scope) are always replaced wholesale by their result's value, unlike
    // regular attributes there is no per-datasetId instance array to diff against
    val membersWithCoreUpdates = coreResults.fold(members) { currentMembers, result ->
        val newValue = result.newExpandedValue.getValue(result.attributeName) as List<*>
        if (newValue.isEmpty()) currentMembers - result.attributeName
        else currentMembers + (result.attributeName to newValue)
    }

    val updatedMembers = attributeResults.groupBy { it.attributeName }
        .entries.fold(membersWithCoreUpdates) { currentMembers, (attrName, results) ->
            val upsertedResults = results.filter { it.operationStatus != OperationStatus.DELETED }
            val excludedDatasetIds = results.map { it.datasetId }.toSet()
            val existingInstances = (currentMembers[attrName] as? ExpandedAttributeInstances).orEmpty()
            val keptInstances = existingInstances.filterNot { it.getDatasetId() in excludedDatasetIds }
            val mergedInstances = keptInstances + upsertedResults.map { it.newExpandedValue }
            if (mergedInstances.isEmpty()) currentMembers - attrName
            else currentMembers + (attrName to mergedInstances)
        }
    return ExpandedEntity(updatedMembers + (NGSILD_MODIFIED_AT_IRI to buildNonReifiedTemporalValue(modifiedAt)))
}
