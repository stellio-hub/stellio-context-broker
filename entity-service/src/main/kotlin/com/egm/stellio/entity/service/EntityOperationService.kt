package com.egm.stellio.entity.service

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.web.BatchEntityError
import com.egm.stellio.entity.web.BatchOperationResult
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NgsiLdEntity
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI

/**
 * Service to work on a list of entities.
 */
@Component
class EntityOperationService(
    private val neo4jRepository: Neo4jRepository,
    private val entityService: EntityService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Splits [entities] by their existence in the DB.
     */
    fun splitEntitiesByExistence(entities: List<NgsiLdEntity>): Pair<List<NgsiLdEntity>, List<NgsiLdEntity>> {
        val extractIdFunc: (NgsiLdEntity) -> URI = { it.id }
        return splitEntitiesByExistenceGeneric(entities, extractIdFunc)
    }

    /**
     * Splits [entityIds] by their existence in the DB.
     */
    fun splitEntitiesIdsByExistence(entityIds: List<URI>): Pair<List<URI>, List<URI>> {
        val identityFunc: (URI) -> URI = { it }
        return splitEntitiesByExistenceGeneric(entityIds, identityFunc)
    }

    private fun <T> splitEntitiesByExistenceGeneric(
        entities: List<T>,
        extractIdFunc: (T) -> URI
    ): Pair<List<T>, List<T>> {
        val existingEntitiesIds = neo4jRepository.filterExistingEntitiesAsIds(entities.map { extractIdFunc.invoke(it) })
        return entities.partition { existingEntitiesIds.contains(extractIdFunc.invoke(it)) }
    }

    /**
     * Creates a batch of [entities].
     *
     * @return a [BatchOperationResult]
     */
    fun create(entities: List<NgsiLdEntity>): BatchOperationResult {
        val createdIdsResults = entities.map { it to kotlin.runCatching { entityService.createEntity(it) } }

        val (successfullyCreatedIdsWithResults, failedToCreateIdsWithResults) =
            createdIdsResults.partition { (_, value) -> value.isSuccess }

        val successfullyCreatedIds = successfullyCreatedIdsWithResults.map { it.first.id }.toMutableList()

        val errors = failedToCreateIdsWithResults
            .onEach { logger.warn("Failed to create entity with id {}", it.first.id, it.second.exceptionOrNull()) }
            .map {
                BatchEntityError(
                    it.first.id,
                    arrayListOf(it.second.exceptionOrNull()?.message ?: "Unknown error while creating entity")
                )
            }
            .toMutableList()

        return BatchOperationResult(successfullyCreatedIds, errors)
    }

    fun delete(entitiesIds: Set<URI>): BatchOperationResult {
        val deletedIdsResults = entitiesIds.map { it to kotlin.runCatching { entityService.deleteEntity(it) } }

        val (successfullyDeletedIdsWithResults, failedToDeleteIdsWithResults) = deletedIdsResults
            .partition { (_, value) -> value.isSuccess }

        val successfullyDeletedIds = successfullyDeletedIdsWithResults.map { it.first }.toMutableList()

        val errors = failedToDeleteIdsWithResults
            .onEach { logger.warn("Failed to delete entity with id {}", it.first, it.second.exceptionOrNull()) }
            .map {
                BatchEntityError(
                    it.first,
                    arrayListOf(it.second.exceptionOrNull()?.message ?: "Unknown error while deleting entity")
                )
            }
            .toMutableList()

        return BatchOperationResult(successfullyDeletedIds, errors)
    }

    /**
     * Replaces a batch of [entities]
     *
     * @return a [BatchOperationResult] with list of replaced ids and list of errors.
     */
    fun replace(entities: List<NgsiLdEntity>): BatchOperationResult {
        return processEntities(entities, ::replaceEntity)
    }

    /**
     * Updates a batch of [entities].
     *
     * @return a [BatchOperationResult] with list of updated ids and list of errors.
     */
    fun update(entities: List<NgsiLdEntity>): BatchOperationResult {
        return processEntities(entities, ::updateEntity)
    }

    private fun processEntities(
        entities: List<NgsiLdEntity>,
        processor: (NgsiLdEntity) -> Either<BatchEntityError, URI>
    ): BatchOperationResult {
        return entities.parallelStream().map {
            processEntity(it, processor)
        }.collect(
            { BatchOperationResult() },
            { batchOperationResult, updateResult ->
                updateResult.fold(
                    {
                        batchOperationResult.errors.add(it)
                    },
                    {
                        batchOperationResult.success.add(it)
                    }
                )
            },
            BatchOperationResult::plusAssign
        )
    }

    private fun processEntity(
        entity: NgsiLdEntity,
        processor: (NgsiLdEntity) -> Either<BatchEntityError, URI>
    ): Either<BatchEntityError, URI> {
        return try {
            processor(entity)
        } catch (e: BadRequestDataException) {
            BatchEntityError(entity.id, arrayListOf(e.message)).left()
        }
    }

    /*
     * Transactional because it should not delete entity attributes if new ones could not be appended.
     */
    @Transactional(rollbackFor = [BadRequestDataException::class])
    @Throws(BadRequestDataException::class)
    fun replaceEntity(entity: NgsiLdEntity): Either<BatchEntityError, URI> {
        neo4jRepository.deleteEntityAttributes(entity.id)
        val (_, notUpdated) = entityService.appendEntityAttributes(entity.id, entity.attributes, false)
        if (notUpdated.isEmpty()) {
            return entity.id.right()
        } else {
            throw BadRequestDataException(
                ArrayList(notUpdated.map { it.attributeName + " : " + it.reason }).joinToString()
            )
        }
    }

    private fun updateEntity(entity: NgsiLdEntity): Either<BatchEntityError, URI> {
        val (_, notUpdated) = entityService.appendEntityAttributes(entity.id, entity.attributes, false)

        return if (notUpdated.isEmpty()) {
            entity.id.right()
        } else {
            BatchEntityError(
                entity.id,
                ArrayList(notUpdated.map { it.attributeName + " : " + it.reason })
            ).left()
        }
    }
}
