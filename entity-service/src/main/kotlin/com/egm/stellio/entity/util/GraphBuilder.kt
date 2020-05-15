package com.egm.stellio.entity.util

import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.entity.web.BatchEntityError
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.google.common.graph.Graph
import com.google.common.graph.GraphBuilder
import org.springframework.stereotype.Component

/**
 * Builder for [Graph].
 */
@Component
class GraphBuilder(
    private val entityService: EntityService
) {

    /**
     * Builds a graph based on given [entities].
     *
     * @return a [Pair] :  a [Graph] of [ExpandedEntity] and a [List] of [BatchEntityError]
     */
    fun build(entities: List<ExpandedEntity>): Pair<Graph<ExpandedEntity>, List<BatchEntityError>> {
        val left = entities.toMutableList()
        val graph = GraphBuilder.directed().build<ExpandedEntity>()
        val errors = mutableListOf<BatchEntityError>()

        @Throws(BadRequestDataException::class)
        fun iterateOverRelationships(entity: ExpandedEntity) {
            if (!left.remove(entity)) {
                return
            }

            try {

                /*
                 * If relationships targets an entity in DB, it is correct and doesn't need to be validated
                 * nor added to the graph.
                 */

                val relationships = getValidRelationshipsFromEntities(entity, entities)

                graph.addNode(entity)

                relationships.forEach {
                    iterateOverRelationships(it)
                    graph.putEdge(entity, it)
                }
            } catch (e: BadRequestDataException) {
                graph.removeNode(entity)
                errors.add(BatchEntityError(entity.id, arrayListOf(e.message)))
                // pass exception to remove parent nodes that don't need to be created because related to an invalid node
                throw e
            }
        }

        while (left.isNotEmpty()) {
            try {
                iterateOverRelationships(left.first())
            } // catch exception already taken into account.
            catch (e: BadRequestDataException) {
            }
        }

        return Pair(graph, errors)
    }

    private fun getValidRelationshipsFromEntities(entity: ExpandedEntity, entities: List<ExpandedEntity>): List<ExpandedEntity> {
        return entity.getRelationships().filter {
            !entityService.exists(it)
        }.map {
            entities.find { entity -> entity.id == it }
                ?: throw BadRequestDataException("Target entity $it does not exist")
        }
    }
}