package com.egm.stellio.search.entity.service

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.web.*
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.Sub
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI

/**
 * Service to work on a list of entities.
 */
@Component
class EntityOperationService(
    private val entityService: EntityService,
    private val entityQueryService: EntityQueryService,
    private val entityAttributeService: EntityAttributeService,
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
    suspend fun create(
        entities: List<JsonLdNgsiLdEntity>,
        sub: Sub?
    ): BatchOperationResult {
        val creationResults = entities.map { jsonLdNgsiLdEntity ->
            either {
                entityService.createEntity(jsonLdNgsiLdEntity.second, jsonLdNgsiLdEntity.first, sub).map {
                    BatchEntitySuccess(jsonLdNgsiLdEntity.entityId())
                }.mapLeft { apiException ->
                    BatchEntityError(jsonLdNgsiLdEntity.entityId(), arrayListOf(apiException.message))
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

    suspend fun delete(entitiesId: List<URI>, sub: Sub?): BatchOperationResult {
        val deletionResults = entitiesId.map { id ->
            either {
                entityService.deleteEntity(id, sub)
                    .map {
                        BatchEntitySuccess(id)
                    }
                    .mapLeft { apiException ->
                        BatchEntityError(id, arrayListOf(apiException.message))
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
    suspend fun replace(entities: List<JsonLdNgsiLdEntity>, sub: Sub?): BatchOperationResult =
        processEntities(entities, false, sub, ::replaceEntity)

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
        disallowOverwrite: Boolean = false,
        sub: Sub?
    ): BatchOperationResult =
        processEntities(entities, disallowOverwrite, sub, ::updateEntity)

    /**
     * Merge a batch of [entities]
     *
     * @return a [BatchOperationResult] with list of updated ids and list of errors.
     */
    @Transactional
    suspend fun merge(
        entities: List<JsonLdNgsiLdEntity>,
        sub: Sub?
    ): BatchOperationResult =
        processEntities(entities, false, sub, ::mergeEntity)

    internal suspend fun processEntities(
        entities: List<JsonLdNgsiLdEntity>,
        disallowOverwrite: Boolean = false,
        sub: Sub?,
        processor:
        suspend (JsonLdNgsiLdEntity, Boolean, Sub?) -> Either<APIException, UpdateResult>
    ): BatchOperationResult =
        entities.map {
            processEntity(it, disallowOverwrite, sub, processor)
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
        sub: Sub?,
        processor:
        suspend (JsonLdNgsiLdEntity, Boolean, Sub?) -> Either<APIException, UpdateResult>
    ): Either<BatchEntityError, BatchEntitySuccess> =
        kotlin.runCatching {
            either {
                val result = processor(entity, disallowOverwrite, sub).bind()
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
                BatchEntityError(entity.entityId(), arrayListOf(it.message))
            }
        }.fold(
            onFailure = { BatchEntityError(entity.entityId(), arrayListOf(it.message!!)).left() },
            onSuccess = { it }
        )

    suspend fun replaceEntity(
        entity: JsonLdNgsiLdEntity,
        disallowOverwrite: Boolean,
        sub: Sub?
    ): Either<APIException, UpdateResult> = either {
        val (jsonLdEntity, ngsiLdEntity) = entity
        entityAttributeService.deleteAttributes(ngsiLdEntity.id).bind()
        entityService.appendAttributes(
            ngsiLdEntity.id,
            jsonLdEntity.getModifiableMembers(),
            disallowOverwrite,
            sub
        ).bind()
    }

    suspend fun updateEntity(
        entity: JsonLdNgsiLdEntity,
        disallowOverwrite: Boolean,
        sub: Sub?
    ): Either<APIException, UpdateResult> = either {
        val (jsonLdEntity, ngsiLdEntity) = entity
        entityService.appendAttributes(
            ngsiLdEntity.id,
            jsonLdEntity.getModifiableMembers(),
            disallowOverwrite,
            sub
        ).bind()
    }

    @SuppressWarnings("UnusedParameter")
    suspend fun mergeEntity(
        entity: JsonLdNgsiLdEntity,
        disallowOverwrite: Boolean,
        sub: Sub?
    ): Either<APIException, UpdateResult> = either {
        val (jsonLdEntity, ngsiLdEntity) = entity
        entityService.mergeEntity(
            ngsiLdEntity.id,
            jsonLdEntity.getModifiableMembers(),
            null,
            sub
        ).bind()
    }
}
