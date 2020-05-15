package com.egm.stellio.entity.service

import com.egm.stellio.entity.util.GraphBuilder
import com.egm.stellio.entity.web.BatchEntityError
import com.egm.stellio.entity.web.BatchOperationResult
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_ID
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_TYPE
import com.google.common.graph.Graph
import com.google.common.graph.Graphs.copyOf
import org.springframework.stereotype.Component
import kotlin.streams.toList

/**
 * Service to work on a list of entities.
 */
@Component
class EntityOperationService(
    private val entityService: EntityService,
    private val graphBuilder: GraphBuilder
) {

    /**
     * Splits [entities] by their existence in the DB.
     */
    fun splitEntitiesByExistence(entities: List<ExpandedEntity>): Pair<List<ExpandedEntity>, List<ExpandedEntity>> =
        entities.partition {
            entityService.exists(it.id)
        }

    /**
     * Creates a batch of [entities].
     *
     * @return a [BatchOperationResult]
     */
    fun create(entities: List<ExpandedEntity>): BatchOperationResult {
        val (graph, invalidRelationsErrors) = graphBuilder.build(entities)

        val (naiveBatchOperationResult, entitiesLeft) = createEntitiesWithoutCircularDependencies(graph)
        val (createSuccess, circularCreateErrors) = createEntitiesWithCircularDependencies(entitiesLeft.toList())

        val success = naiveBatchOperationResult.success.plus(createSuccess)
        val errors = invalidRelationsErrors.plus(naiveBatchOperationResult.errors).plus(circularCreateErrors)

        return BatchOperationResult(ArrayList(success), ArrayList(errors))
    }

    private fun createEntitiesWithoutCircularDependencies(graph: Graph<ExpandedEntity>): Pair<BatchOperationResult, MutableSet<ExpandedEntity>> {
        val batchOperationResult = BatchOperationResult(arrayListOf(), arrayListOf())
        val mutableGraph = copyOf(graph)

        /**
         * Gets a list of leaves, with 0 successors.
         */
        fun <T> Graph<T>.getLeaves(): List<T> = nodes().filter { outDegree(it) == 0 }

        do {
            val leaves = mutableGraph.getLeaves()

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
                mutableGraph.removeNode(it)
            }
        } while (leaves.isNotEmpty())

        return Pair(batchOperationResult, mutableGraph.nodes())
    }

    /*
     * Creates given entities into database.
     * When there are circular dependencies between nodes, the creation should be achieved in 2 steps:
     * first create a "temp" entity with almost no attributes
     * then create the relationships on these attributes
     */
    private fun createEntitiesWithCircularDependencies(entities: List<ExpandedEntity>): BatchOperationResult {
        entities.forEach { entity ->
            entityService.createTempEntityInBatch(entity.id, entity.type, entity.contexts)
        }

        // TODO improve process, if an entity update fails, we should check linked entities to also not delete them
        return entities.fold(BatchOperationResult(arrayListOf(), arrayListOf()), { (creations, errors), entity ->
            try {
                entityService.appendEntityAttributes(
                    entity.id,
                    entity.attributes.filterKeys {
                        !listOf(NGSILD_ENTITY_ID, NGSILD_ENTITY_TYPE).contains(it)
                    },
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
}
