package com.egm.stellio.entity.service

import arrow.core.Either
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.repository.EntityRepository
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.util.EntitiesGraphBuilder
import com.egm.stellio.entity.web.BatchEntityError
import com.egm.stellio.entity.web.BatchOperationResult
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import org.jgrapht.Graph
import org.jgrapht.Graphs
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.DirectedPseudograph
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import kotlin.streams.toList

/**
 * Service to work on a list of entities.
 */
@Component
class EntityOperationService(
    private val neo4jRepository: Neo4jRepository,
    private val entityRepository: EntityRepository,
    private val entityService: EntityService,
    private val entitiesGraphBuilder: EntitiesGraphBuilder
) {

    /**
     * Splits [entities] by their existence in the DB.
     */
    fun splitEntitiesByExistence(entities: List<ExpandedEntity>): Pair<List<ExpandedEntity>, List<ExpandedEntity>> {
        val existingEntitiesIds =
            neo4jRepository.filterExistingEntitiesIds(entities.map { it.id })
        return entities.partition {
            existingEntitiesIds.contains(it.id)
        }
    }

    /**
     * Creates a batch of [entities].
     *
     * @return a [BatchOperationResult]
     */
    fun create(entities: List<ExpandedEntity>): BatchOperationResult {
        val (graph, invalidRelationsErrors) = entitiesGraphBuilder.build(entities)

        val (naiveBatchResult, entitiesWithCircularDependencies) = createEntitiesWithoutCircularDependencies(graph)
        val (circularCreateSuccess, circularCreateErrors) = createEntitiesWithCircularDependencies(
            entitiesWithCircularDependencies.toList()
        )

        val success = naiveBatchResult.success.plus(circularCreateSuccess)
        val errors = invalidRelationsErrors.plus(naiveBatchResult.errors).plus(circularCreateErrors)

        return BatchOperationResult(ArrayList(success), ArrayList(errors))
    }

    /**
     * Replaces a batch of [entities]
     * Only entities with relations linked to existing entities will be replaced.
     *
     * @return a [BatchOperationResult] with list of replaced ids and list of errors (either not replaced or
     * linked to invalid entity).
     */
    fun replace(entities: List<ExpandedEntity>, createBatchResult: BatchOperationResult): BatchOperationResult {
        return processEntities(entities, createBatchResult, ::replaceEntity)
    }

    /**
     * Updates a batch of [entities].
     * Only entities with relations linked to existing entities will be updated.
     *
     * @return a [BatchOperationResult] with list of updated ids and list of errors (either not totally updated or
     * linked to invalid entity).
     */
    fun update(entities: List<ExpandedEntity>, createBatchResult: BatchOperationResult): BatchOperationResult {
        return processEntities(entities, createBatchResult, ::updateEntity)
    }

    private fun processEntities(
        entities: List<ExpandedEntity>,
        createBatchResult: BatchOperationResult,
        processor: (ExpandedEntity) -> Either<BatchEntityError, String>
    ): BatchOperationResult {
        val existingEntitiesIds = createBatchResult.success.plus(entities.map { it.id })
        val nonExistingEntitiesIds = createBatchResult.errors.map { it.entityId }
        return entities.parallelStream().map {
            processEntity(it, processor, existingEntitiesIds, nonExistingEntitiesIds)
        }.collect(
            { BatchOperationResult() },
            { batchOperationResult, updateResult ->
                updateResult.fold({
                    batchOperationResult.errors.add(it)
                }, {
                    batchOperationResult.success.add(it)
                })
            },
            BatchOperationResult::plusAssign
        )
    }

    private fun processEntity(
        entity: ExpandedEntity,
        processor: (ExpandedEntity) -> Either<BatchEntityError, String>,
        existingEntitiesIds: List<String>,
        nonExistingEntitiesIds: List<String>
    ): Either<BatchEntityError, String> {
        // All new attributes linked entities should be existing in the DB.
        val linkedEntitiesIds = entity.getLinkedEntitiesIds()
        val invalidLinkedEntityId =
            findInvalidEntityId(linkedEntitiesIds, existingEntitiesIds, nonExistingEntitiesIds)

        // If there's a link to an invalid entity, then avoid calling the processor and return an error
        if (invalidLinkedEntityId != null) {
            return Either.left(
                BatchEntityError(
                    entity.id,
                    arrayListOf("Target entity $invalidLinkedEntityId does not exist.")
                )
            )
        }

        return try {
            processor(entity)
        } catch (e: BadRequestDataException) {
            Either.left(BatchEntityError(entity.id, arrayListOf(e.message)))
        }
    }

    /*
     * Transactional because it should not delete entity attributes if new ones could not be appended.
     */
    @Transactional(rollbackFor = [BadRequestDataException::class])
    @Throws(BadRequestDataException::class)
    private fun replaceEntity(entity: ExpandedEntity): Either<BatchEntityError, String> {
        neo4jRepository.deleteEntityAttributes(entity.id)
        val (_, notUpdated) = entityService.appendEntityAttributes(entity.id, entity.attributes, false)
        if (notUpdated.isEmpty()) {
            return Either.right(entity.id)
        } else {
            throw BadRequestDataException(ArrayList(notUpdated.map { it.attributeName + " : " + it.reason }).joinToString())
        }
    }

    private fun updateEntity(entity: ExpandedEntity): Either<BatchEntityError, String> {
        val (_, notUpdated) = entityService.appendEntityAttributes(
            entity.id,
            entity.attributes,
            false
        )

        return if (notUpdated.isEmpty()) {
            Either.right(entity.id)
        } else {
            Either.left(
                BatchEntityError(
                    entity.id,
                    ArrayList(notUpdated.map { it.attributeName + " : " + it.reason })
                )
            )
        }
    }

    private fun findInvalidEntityId(
        entitiesIds: List<String>,
        existingEntitiesIds: List<String>,
        nonExistingEntitiesIds: List<String>
    ): String? {
        val invalidEntityId = entitiesIds.intersect(nonExistingEntitiesIds).firstOrNull()
        if (invalidEntityId == null) {
            val unknownEntitiesIds = entitiesIds.minus(existingEntitiesIds)
            return unknownEntitiesIds
                .minus(neo4jRepository.filterExistingEntitiesIds(unknownEntitiesIds)).firstOrNull()
        }
        return invalidEntityId
    }

    private fun createEntitiesWithoutCircularDependencies(graph: Graph<ExpandedEntity, DefaultEdge>): Pair<BatchOperationResult, Set<ExpandedEntity>> {
        val batchOperationResult = BatchOperationResult(arrayListOf(), arrayListOf())
        val temporaryGraph = DirectedPseudograph<ExpandedEntity, DefaultEdge>(DefaultEdge::class.java)
        Graphs.addGraph(temporaryGraph, graph)

        /**
         * Gets a list of leaves (i.e. with 0 successors).
         */
        fun <T, E> Graph<T, E>.getLeaves(): List<T> = vertexSet().filter { outDegreeOf(it) == 0 }

        do {
            val leaves = temporaryGraph.getLeaves()

            val res = leaves.parallelStream().map {
                try {
                    entityService.createEntity(it)
                    Pair(it.id, null)
                } catch (e: BadRequestDataException) {
                    Pair(null, BatchEntityError(it.id, arrayListOf(e.message)))
                }
            }

            res.toList().fold(batchOperationResult, { result, (entity, error) ->
                entity?.let { result.success.add(it) }
                error?.let { result.errors.add(it) }
                result
            })

            leaves.forEach {
                temporaryGraph.removeVertex(it)
            }
        } while (leaves.isNotEmpty())

        return Pair(batchOperationResult, temporaryGraph.vertexSet())
    }

    /*
     * Creates given entities into database.
     * When there are circular dependencies between nodes, the creation should be achieved in 2 steps:
     * first create a "temp" entity with almost no attributes
     * then create the relationships on these attributes
     */
    private fun createEntitiesWithCircularDependencies(entities: List<ExpandedEntity>): BatchOperationResult {
        entities.forEach { entity ->
            createTempEntityInBatch(entity.id, entity.type, entity.contexts)
        }

        // TODO improve process, if an entity update fails, we should check linked entities to also not delete them
        return entities.fold(BatchOperationResult(arrayListOf(), arrayListOf()), { (creations, errors), entity ->
            try {
                entityService.appendEntityAttributes(
                    entity.id,
                    entity.attributes,
                    false
                )

                creations.add(entity.id)
                entityService.publishCreationEvent(entity)
            } catch (e: BadRequestDataException) {
                entityService.deleteEntity(entity.id)
                errors.add(BatchEntityError(entity.id, arrayListOf(e.message)))
            }

            BatchOperationResult(creations, errors)
        })
    }

    private fun createTempEntityInBatch(
        entityId: String,
        entityType: String,
        contexts: List<String> = listOf()
    ): Entity {
        val entity = Entity(id = entityId, type = listOf(entityType), contexts = contexts)
        entityRepository.save(entity)
        return entity
    }
}
