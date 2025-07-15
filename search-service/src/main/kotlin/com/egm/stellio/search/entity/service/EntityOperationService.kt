package com.egm.stellio.search.entity.service

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.entity.model.EMPTY_UPDATE_RESULT
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.web.BatchEntityError
import com.egm.stellio.search.entity.web.BatchEntitySuccess
import com.egm.stellio.search.entity.web.BatchOperationResult
import com.egm.stellio.search.entity.web.JsonLdNgsiLdEntity
import com.egm.stellio.search.entity.web.entityId
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.toAPIException
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI

/**
 * Service to work on a list of entities.
 */
@Component
class EntityOperationService(
    private val entityService: EntityService,
    private val entityQueryService: EntityQueryService
) {

    /**
     * Splits [entities] by their existence in the DB.
     */
    suspend fun splitEntitiesByExistence(
        entities: List<JsonLdNgsiLdEntity>
    ): Pair<List<JsonLdNgsiLdEntity>, List<JsonLdNgsiLdEntity>> {
        val extractIdFunc: (JsonLdNgsiLdEntity) -> URI = { it.entityId() }
        return splitEntitiesByExistenceGeneric(entities, extractIdFunc)
    }

    /**
     * Splits [entityIds] by their existence in the DB.
     */
    suspend fun splitEntitiesIdsByExistence(entityIds: List<URI>): Pair<List<URI>, List<URI>> {
        val identityFunc: (URI) -> URI = { it }
        return splitEntitiesByExistenceGeneric(entityIds, identityFunc)
    }

    private suspend fun <T> splitEntitiesByExistenceGeneric(
        entities: List<T>,
        extractIdFunc: (T) -> URI
    ): Pair<List<T>, List<T>> {
        val existingEntitiesIds =
            entityQueryService.filterExistingEntitiesAsIds(entities.map { extractIdFunc.invoke(it) })
        return entities.partition { existingEntitiesIds.contains(extractIdFunc.invoke(it)) }
    }

    fun splitEntitiesByUniqueness(
        entities: List<JsonLdNgsiLdEntity>
    ): Pair<List<JsonLdNgsiLdEntity>, List<JsonLdNgsiLdEntity>> {
        val extractIdFunc: (JsonLdNgsiLdEntity) -> URI = { it.entityId() }
        return splitEntitiesByUniquenessGeneric(entities, extractIdFunc)
    }

    fun splitEntitiesIdsByUniqueness(entityIds: List<URI>): Pair<List<URI>, List<URI>> {
        val identityFunc: (URI) -> URI = { it }
        return splitEntitiesByUniquenessGeneric(entityIds, identityFunc)
    }

    fun <T> splitEntitiesByUniquenessGeneric(
        entities: List<T>,
        extractIdFunc: (T) -> URI
    ): Pair<List<T>, List<T>> =
        entities.fold(Pair(emptyList(), emptyList())) { acc, current ->
            if (acc.first.any { extractIdFunc(it) == extractIdFunc(current) })
                Pair(acc.first, acc.second.plus(current))
            else Pair(acc.first.plus(current), acc.second)
        }

    /**
     * Creates a batch of [entities].
     *
     * @return a [BatchOperationResult]
     */
    suspend fun create(entities: List<JsonLdNgsiLdEntity>): BatchOperationResult {
        val creationResults = entities.map { jsonLdNgsiLdEntity ->
            either {
                entityService.createEntity(jsonLdNgsiLdEntity.second, jsonLdNgsiLdEntity.first).map {
                    BatchEntitySuccess(jsonLdNgsiLdEntity.entityId())
                }.mapLeft { apiException ->
                    BatchEntityError(jsonLdNgsiLdEntity.entityId(), apiException.toProblemDetail())
                }.bind()
            }
        }.fold(
            initial = Pair(listOf<BatchEntityError>(), listOf<BatchEntitySuccess>()),
            operation = { acc, either ->
                either.fold(
                    ifLeft = { Pair(acc.first.plus(it), acc.second) },
                    ifRight = { Pair(acc.first, acc.second.plus(it)) }
                )
            }
        )

        return BatchOperationResult(creationResults.second.toMutableList(), creationResults.first.toMutableList())
    }

    suspend fun delete(entitiesId: List<URI>): BatchOperationResult {
        val deletionResults = entitiesId.map { id ->
            either {
                entityService.deleteEntity(id)
                    .map {
                        BatchEntitySuccess(id)
                    }
                    .mapLeft { apiException ->
                        BatchEntityError(id, apiException.toProblemDetail())
                    }.bind()
            }
        }.fold(
            initial = Pair(listOf<BatchEntityError>(), listOf<BatchEntitySuccess>()),
            operation = { acc, either ->
                either.fold(
                    ifLeft = { Pair(acc.first.plus(it), acc.second) },
                    ifRight = { Pair(acc.first, acc.second.plus(it)) }
                )
            }
        )

        return BatchOperationResult(
            deletionResults.second.toMutableList(),
            deletionResults.first.toMutableList()
        )
    }

    /**
     * Replaces a batch of [entities]
     *
     * @return a [BatchOperationResult] with list of replaced ids and list of errors.
     */
    @Transactional
    suspend fun replace(entities: List<JsonLdNgsiLdEntity>): BatchOperationResult =
        processEntities(entities, processor = ::replaceEntity)

    /**
     * Upsert a batch of [entities]
     *
     * @return a [BatchOperationResult] with list of replaced ids and list of errors.
     */
    @Transactional
    suspend fun upsert(
        entities: List<JsonLdNgsiLdEntity>,
        disallowOverwrite: Boolean,
        updateMode: Boolean
    ): Pair<BatchOperationResult, List<URI>> {
        val (existingEntities, newEntities) = splitEntitiesByExistence(entities)

        val (newUniqueEntities, duplicatedEntities) = splitEntitiesByUniqueness(newEntities)
        val existingOrDuplicatedEntities = existingEntities.plus(duplicatedEntities)
        val batchOperationResult = BatchOperationResult()

        val createdIds = if (newUniqueEntities.isNotEmpty()) {
            val createOperationResult = create(newUniqueEntities)
            batchOperationResult.errors.addAll(createOperationResult.errors)
            batchOperationResult.success.addAll(createOperationResult.success)
            createOperationResult.success.map { it.entityId }
        } else emptyList()

        if (existingOrDuplicatedEntities.isNotEmpty()) {
            val updateOperationResult =
                if (updateMode) update(existingOrDuplicatedEntities, disallowOverwrite)
                else replace(existingOrDuplicatedEntities)

            batchOperationResult.errors.addAll(updateOperationResult.errors)
            batchOperationResult.success.addAll(updateOperationResult.success)
        }
        return batchOperationResult to createdIds
    }

    /**
     * Updates a batch of [entities]
     *
     * @param disallowOverwrite whether overwriting existing attributes is allowed
     *
     * @return a [BatchOperationResult] with list of updated ids and list of errors.
     */
    @Transactional
    suspend fun update(
        entities: List<JsonLdNgsiLdEntity>,
        disallowOverwrite: Boolean = false
    ): BatchOperationResult =
        processEntities(entities, disallowOverwrite, ::updateEntity)

    /**
     * Merge a batch of [entities]
     *
     * @return a [BatchOperationResult] with list of updated ids and list of errors.
     */
    @Transactional
    suspend fun merge(
        entities: List<JsonLdNgsiLdEntity>
    ): BatchOperationResult =
        processEntities(entities, processor = ::mergeEntity)

    internal suspend fun processEntities(
        entities: List<JsonLdNgsiLdEntity>,
        disallowOverwrite: Boolean = false,
        processor: suspend (JsonLdNgsiLdEntity, Boolean) -> Either<APIException, UpdateResult>
    ): BatchOperationResult =
        entities.map {
            processEntity(it, disallowOverwrite, processor)
        }.fold(
            initial = BatchOperationResult(),
            operation = { acc, either ->
                either.fold(
                    ifLeft = { acc.copy(errors = acc.errors.plus(it).toMutableList()) },
                    ifRight = { acc.copy(success = acc.success.plus(it).toMutableList()) }
                )
            }
        )

    private suspend fun processEntity(
        entity: JsonLdNgsiLdEntity,
        disallowOverwrite: Boolean = false,
        processor: suspend (JsonLdNgsiLdEntity, Boolean) -> Either<APIException, UpdateResult>
    ): Either<BatchEntityError, BatchEntitySuccess> =
        kotlin.runCatching {
            either {
                val result = processor(entity, disallowOverwrite).bind()
                if (result.notUpdated.isEmpty())
                    result.right().bind()
                else
                    BadRequestDataException(
                        ArrayList(result.notUpdated.map { it.attributeName + " : " + it.reason })
                            .joinToString()
                    ).left().bind<UpdateResult>()
            }.map {
                BatchEntitySuccess(entity.entityId(), it)
            }.mapLeft {
                BatchEntityError(entity.entityId(), it.toProblemDetail())
            }
        }.fold(
            onFailure = { BatchEntityError(entity.entityId(), it.toAPIException().toProblemDetail()).left() },
            onSuccess = { it }
        )

    @SuppressWarnings("UnusedParameter")
    suspend fun replaceEntity(
        entity: JsonLdNgsiLdEntity,
        disallowOverwrite: Boolean
    ): Either<APIException, UpdateResult> = either {
        val (jsonLdEntity, ngsiLdEntity) = entity
        entityService.replaceEntity(ngsiLdEntity.id, ngsiLdEntity, jsonLdEntity).map {
            EMPTY_UPDATE_RESULT
        }.bind()
    }

    suspend fun updateEntity(
        entity: JsonLdNgsiLdEntity,
        disallowOverwrite: Boolean
    ): Either<APIException, UpdateResult> = either {
        val (jsonLdEntity, ngsiLdEntity) = entity
        entityService.appendAttributes(
            ngsiLdEntity.id,
            jsonLdEntity.getModifiableMembers(),
            disallowOverwrite
        ).bind()
    }

    @SuppressWarnings("UnusedParameter")
    suspend fun mergeEntity(
        entity: JsonLdNgsiLdEntity,
        disallowOverwrite: Boolean
    ): Either<APIException, UpdateResult> = either {
        val (jsonLdEntity, ngsiLdEntity) = entity
        entityService.mergeEntity(
            ngsiLdEntity.id,
            jsonLdEntity.getModifiableMembers(),
            null
        ).bind()
    }
}
