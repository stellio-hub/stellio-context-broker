package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.search.web.*
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
    private val entityPayloadService: EntityPayloadService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val authorizationService: AuthorizationService
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
            entityPayloadService.filterExistingEntitiesAsIds(entities.map { extractIdFunc.invoke(it) })
        return entities.partition { existingEntitiesIds.contains(extractIdFunc.invoke(it)) }
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
                entityPayloadService.createEntity(jsonLdNgsiLdEntity.second, jsonLdNgsiLdEntity.first, sub)
                    .map {
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

    suspend fun delete(entitiesIds: Set<URI>): BatchOperationResult {
        val deletionResults = entitiesIds.map {
            val entityId = it
            either {
                entityPayloadService.deleteEntity(entityId)
                    .map {
                        authorizationService.removeRightsOnEntity(entityId)
                    }
                    .map {
                        BatchEntitySuccess(entityId)
                    }
                    .mapLeft { apiException ->
                        BatchEntityError(entityId, arrayListOf(apiException.message))
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

    private suspend fun processEntities(
        entities: List<JsonLdNgsiLdEntity>,
        disallowOverwrite: Boolean = false,
        sub: Sub?,
        processor:
        suspend (JsonLdNgsiLdEntity, Boolean, Sub?) -> Either<BatchEntityError, BatchEntitySuccess>
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
        suspend (JsonLdNgsiLdEntity, Boolean, Sub?) -> Either<BatchEntityError, BatchEntitySuccess>
    ): Either<BatchEntityError, BatchEntitySuccess> =
        kotlin.runCatching {
            processor(entity, disallowOverwrite, sub)
        }.fold(
            onFailure = { BatchEntityError(entity.entityId(), arrayListOf(it.message!!)).left() },
            onSuccess = { it }
        )

    /*
     * Transactional because it should not delete entity attributes if new ones could not be appended.
     */
    @Transactional(rollbackFor = [BadRequestDataException::class])
    @Throws(BadRequestDataException::class)
    suspend fun replaceEntity(
        entity: JsonLdNgsiLdEntity,
        disallowOverwrite: Boolean,
        sub: Sub?
    ): Either<BatchEntityError, BatchEntitySuccess> = either {
        val (jsonLdEntity, ngsiLdEntity) = entity
        temporalEntityAttributeService.deleteTemporalAttributesOfEntity(ngsiLdEntity.id)
        val updateResult = entityPayloadService.appendAttributes(
            ngsiLdEntity.id,
            jsonLdEntity.getAttributes(),
            disallowOverwrite,
            sub
        ).bind()

        if (updateResult.notUpdated.isNotEmpty())
            BadRequestDataException(
                updateResult.notUpdated.joinToString(", ") { it.attributeName + " : " + it.reason }
            ).left().bind<UpdateResult>()
        else updateResult.right().bind()
    }.map {
        BatchEntitySuccess(entity.entityId())
    }.mapLeft {
        BatchEntityError(entity.entityId(), arrayListOf(it.message))
    }

    /*
     * Transactional because it should not replace entity attributes if new ones could not be replaced.
     */
    @Transactional
    suspend fun updateEntity(
        entity: JsonLdNgsiLdEntity,
        disallowOverwrite: Boolean,
        sub: Sub?
    ): Either<BatchEntityError, BatchEntitySuccess> = either {
        val (jsonLdEntity, ngsiLdEntity) = entity
        val updateResult = entityPayloadService.appendAttributes(
            ngsiLdEntity.id,
            jsonLdEntity.getAttributes(),
            disallowOverwrite,
            sub
        ).bind()
        if (updateResult.notUpdated.isEmpty())
            updateResult.right().bind()
        else
            BadRequestDataException(
                ArrayList(updateResult.notUpdated.map { it.attributeName + " : " + it.reason }).joinToString()
            ).left().bind<UpdateResult>()
    }.map {
        BatchEntitySuccess(entity.entityId(), it)
    }.mapLeft {
        BatchEntityError(entity.entityId(), arrayListOf(it.message))
    }
}
