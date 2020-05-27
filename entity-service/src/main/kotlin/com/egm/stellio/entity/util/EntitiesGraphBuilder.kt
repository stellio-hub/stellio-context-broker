package com.egm.stellio.entity.util

import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.web.BatchEntityError
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.DirectedPseudograph
import org.springframework.stereotype.Component

/**
 * Builder for [Graph].
 */
@Component
class EntitiesGraphBuilder(
    private val neo4jRepository: Neo4jRepository
) {

    /**
     * Builds a graph based on given [entities].
     *
     * @return a graph containing only entities linked to entities in the input or in the DB.
     */
    fun build(entities: List<ExpandedEntity>): Pair<Graph<ExpandedEntity, DefaultEdge>, List<BatchEntityError>> {
        val left = entities.toMutableList()
        val graph = DirectedPseudograph<ExpandedEntity, DefaultEdge>(DefaultEdge::class.java)
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

                val relationships = getValidLinkedEntities(entity, entities)

                graph.addVertex(entity)

                relationships.forEach {
                    iterateOverRelationships(it)
                    graph.addEdge(entity, it)
                }
            } catch (e: BadRequestDataException) {
                graph.removeVertex(entity)
                errors.add(BatchEntityError(entity.id, arrayListOf(e.message)))
                // pass exception to remove parent nodes that don't need to be created because related to an invalid node
                throw BadRequestDataException("Target entity " + entity.id + " failed to be created because of an invalid relationship.")
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

    private fun getValidLinkedEntities(entity: ExpandedEntity, entities: List<ExpandedEntity>): List<ExpandedEntity> {
        val linkedEntitiesIds = entity.getLinkedEntitiesIds()
        val nonExistingLinkedEntitiesIds =
            linkedEntitiesIds.minus(neo4jRepository.filterExistingEntitiesIds(linkedEntitiesIds))
        return nonExistingLinkedEntitiesIds.map {
            entities.find { entity -> entity.id == it }
                ?: throw BadRequestDataException("Target entity $it does not exist.")
        }
    }
}