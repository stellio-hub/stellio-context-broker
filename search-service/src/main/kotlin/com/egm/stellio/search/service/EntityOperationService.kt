package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.search.util.prepareTemporalAttributes
import com.egm.stellio.search.web.BatchEntityError
import com.egm.stellio.search.web.BatchEntitySuccess
import com.egm.stellio.search.web.BatchOperationResult
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NgsiLdEntity
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
    private val temporalEntityAttributeService: TemporalEntityAttributeService
) {

    /**
     * Splits [entities] by their existence in the DB.
     */
    suspend fun splitEntitiesByExistence(entities: List<NgsiLdEntity>): Pair<List<NgsiLdEntity>, List<NgsiLdEntity>> {
        val extractIdFunc: (NgsiLdEntity) -> URI = { it.id }
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
        entities: List<NgsiLdEntity>,
        jsonLdEntities: List<JsonLdEntity>,
        sub: Sub?
    ): BatchOperationResult {
        val creationResults = entities.map {
            val ngsiLdEntity = it
            either<BatchEntityError, BatchEntitySuccess> {
                val jsonLdEntity = jsonLdEntities.find { ngsiLdEntity.id.toString() == it.id }!!
                ngsiLdEntity.prepareTemporalAttributes()
                    .flatMap { attributesMetadata ->
                        temporalEntityAttributeService.createEntityTemporalReferences(
                            ngsiLdEntity, jsonLdEntity, attributesMetadata, sub
                        )
                    }.bimap({ apiException ->
                        BatchEntityError(ngsiLdEntity.id, arrayListOf(apiException.message))
                    }, {
                        BatchEntitySuccess(ngsiLdEntity.id)
                    }).bind()
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
            creationResults.second.toMutableList(), creationResults.first.toMutableList()
        )
    }

    suspend fun delete(entitiesIds: Set<URI>): BatchOperationResult {
        val deletionResults = entitiesIds.map {
            val entityId = it
            either<BatchEntityError, BatchEntitySuccess> {
                temporalEntityAttributeService.deleteTemporalEntityReferences(entityId)
                    .bimap({ apiException ->
                        BatchEntityError(entityId, arrayListOf(apiException.message))
                    }, {
                        BatchEntitySuccess(entityId)
                    }).bind()
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
    suspend fun replace(entities: List<Pair<NgsiLdEntity, JsonLdEntity>>, sub: Sub?): BatchOperationResult =
        processEntities(entities, false, sub, ::replaceEntity)

    /**
     * Updates a batch of [entities]
     *
     * @param disallowOverwrite whether overwriting existing attributes is allowed
     *
     * @return a [BatchOperationResult] with list of updated ids and list of errors.
     */
    suspend fun update(
        entities: List<Pair<NgsiLdEntity, JsonLdEntity>>,
        disallowOverwrite: Boolean = false,
        sub: Sub?
    ): BatchOperationResult =
        processEntities(entities, disallowOverwrite, sub, ::updateEntity)

    private suspend fun processEntities(
        entities: List<Pair<NgsiLdEntity, JsonLdEntity>>,
        disallowOverwrite: Boolean = false,
        sub: Sub?,
        processor:
            suspend (Pair<NgsiLdEntity, JsonLdEntity>, Boolean, Sub?) -> Either<BatchEntityError, BatchEntitySuccess>
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
        entity: Pair<NgsiLdEntity, JsonLdEntity>,
        disallowOverwrite: Boolean = false,
        sub: Sub?,
        processor:
            suspend (Pair<NgsiLdEntity, JsonLdEntity>, Boolean, Sub?) -> Either<BatchEntityError, BatchEntitySuccess>
    ): Either<BatchEntityError, BatchEntitySuccess> =
        kotlin.runCatching {
            processor(entity, disallowOverwrite, sub)
        }.fold(
            onFailure = { BatchEntityError(entity.first.id, arrayListOf(it.message!!)).left() },
            onSuccess = { it }
        )

    /*
     * Transactional because it should not delete entity attributes if new ones could not be appended.
     */
    @Transactional(rollbackFor = [BadRequestDataException::class])
    @Throws(BadRequestDataException::class)
    suspend fun replaceEntity(
        entity: Pair<NgsiLdEntity, JsonLdEntity>,
        disallowOverwrite: Boolean,
        sub: Sub?
    ): Either<BatchEntityError, BatchEntitySuccess> =
        either<APIException, UpdateResult> {
            val (ngsiLdEntity, jsonLdEntity) = entity
            temporalEntityAttributeService.deleteTemporalAttributesOfEntity(ngsiLdEntity.id).bind()
            val updateResult = entityPayloadService.updateTypes(
                ngsiLdEntity.id,
                ngsiLdEntity.types,
                false
            ).bind().mergeWith(
                temporalEntityAttributeService.appendEntityAttributes(
                    ngsiLdEntity.id,
                    ngsiLdEntity.attributes,
                    jsonLdEntity.properties,
                    disallowOverwrite,
                    sub
                ).bind()
            )
            if (updateResult.notUpdated.isNotEmpty())
                BadRequestDataException(
                    updateResult.notUpdated.joinToString(", ") { it.attributeName + " : " + it.reason }
                ).left().bind<UpdateResult>()
            else updateResult.right().bind()
        }.bimap({
            BatchEntityError(entity.first.id, arrayListOf(it.message))
        }, {
            BatchEntitySuccess(entity.first.id)
        })

    /*
     * Transactional because it should not replace entity attributes if new ones could not be replaced.
     */
    @Transactional
    suspend fun updateEntity(
        entity: Pair<NgsiLdEntity, JsonLdEntity>,
        disallowOverwrite: Boolean,
        sub: Sub?
    ): Either<BatchEntityError, BatchEntitySuccess> =
        either<APIException, UpdateResult> {
            val (ngsiLdEntity, jsonLdEntity) = entity
            val updateResult = entityPayloadService.updateTypes(
                ngsiLdEntity.id,
                ngsiLdEntity.types,
                false
            ).bind().mergeWith(
                temporalEntityAttributeService.appendEntityAttributes(
                    ngsiLdEntity.id,
                    ngsiLdEntity.attributes,
                    jsonLdEntity.properties,
                    disallowOverwrite,
                    sub
                ).bind()
            )
            if (updateResult.notUpdated.isEmpty())
                updateResult.right().bind()
            else
                BadRequestDataException(
                    ArrayList(updateResult.notUpdated.map { it.attributeName + " : " + it.reason }).joinToString()
                ).left().bind<UpdateResult>()
        }.bimap({
            BatchEntityError(entity.first.id, arrayListOf(it.message))
        }, {
            BatchEntitySuccess(entity.first.id, it)
        })
}