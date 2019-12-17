package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.model.Property
import org.neo4j.ogm.session.Session
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
class Neo4jRepository(
    private val session: Session
) {
    private val logger = LoggerFactory.getLogger(Neo4jRepository::class.java)

    /**
     * Create a relationship from a property (persisted as an entity) to an entity.
     *
     */
    fun createRelationshipFromProperty(subjectId: String, relationshipType: String, targetId: String): Int {
        val query = """
            MATCH (subject:Property { id: "$subjectId" }), (target:Entity { id: "$targetId" })
            MERGE (subject)-[:$relationshipType]->(target)
            MERGE (subject)-[:HAS_OBJECT]->(target)
        """

        return session.query(query, emptyMap<String, String>()).queryStatistics().relationshipsCreated
    }

    /**
     * Create a relationship from an entity to another entity.
     *
     * It is used for :
     *   - outgoing relationships between entities
     *   - outgoing relationships on relationships (stored as entities).
     */
    fun createRelationshipFromEntity(subjectId: String, relationshipType: String, targetId: String): Int {
        val query = """
            MATCH (subject:Entity { id: "$subjectId" }), (target:Entity { id: "$targetId" })
            MERGE (subject)-[:$relationshipType]->(target)
            MERGE (subject)-[:HAS_OBJECT]->(target)
        """

        return session.query(query, emptyMap<String, String>()).queryStatistics().relationshipsCreated
    }

    /**
     * Add a spatial property to an entity.
     */
    fun addLocationPropertyToEntity(subjectId: String, coordinates: Pair<Double, Double>): Int {
        val query = """
            MERGE (subject:Entity { id: "$subjectId" })
            ON MATCH SET subject.location = point({x: ${coordinates.first}, y: ${coordinates.second}, crs: 'wgs-84'})
        """

        return session.query(query, emptyMap<String, String>()).queryStatistics().propertiesSet
    }

    @Transactional
    fun updateEntityAttribute(entityId: String, propertyName: String, propertyValue: Any): Int {
        val query = """
            MERGE (entity:Entity { id: "$entityId" })-[:HAS_VALUE]->(property:Property { name: "$propertyName" })
            ON MATCH SET property.value = $propertyValue
        """.trimIndent()

        return session.query(query, emptyMap<String, String>()).queryStatistics().propertiesSet
    }

    @Transactional
    fun replaceEntityAttribute(entityId: String, propertyName: String, value: Any): Int {
        val query = """
            MERGE (entity:Entity { id: "$entityId" })-[:HAS_VALUE]->(property:Property { name: "$propertyName" })
            ON MATCH SET property.value = $value
        """.trimIndent()

        return session.query(query, emptyMap<String, String>()).queryStatistics().propertiesSet
    }

    @Transactional
    fun deleteRelationshipFromEntity(entityId: String, relationshipType: String): Int {
        val query = """
            MATCH (n:Entity { id: '$entityId' })-[:HAS_OBJECT]->(rel)-[:$relationshipType]->()
            DETACH DELETE rel 
        """.trimIndent()

        return session.query(query, emptyMap<String, String>()).queryStatistics().nodesDeleted
    }

    @Transactional
    fun deletePropertyFromEntity(entityId: String, propertyName: String): Int {
        val query = """
            MATCH (n:Entity { id: '$entityId' })-[:HAS_VALUE]->(property:Property { name: "$propertyName" })
            DETACH DELETE property
        """.trimIndent()

        return session.query(query, emptyMap<String, String>()).queryStatistics().nodesDeleted
    }

    fun deleteEntity(entityId: String): Pair<Int, Int> {
        /**
         * 1. delete the entity node
         * 2. delete the properties (persisted as nodes)
         * 3. delete the relationships
         * 4. delete the relationships on relationships persisted as nodes
         */
        val query = """
            MATCH (n:Entity { id: '$entityId' }) 
            OPTIONAL MATCH (n)-[:HAS_VALUE]->(prop)
            OPTIONAL MATCH (n)-[:HAS_OBJECT]->(rel)
            DETACH DELETE n,prop,rel
        """.trimIndent()

        val queryStatistics = session.query(query, emptyMap<String, Any>()).queryStatistics()
        logger.debug("Deleted entity $entityId : deleted ${queryStatistics.nodesDeleted} nodes, ${queryStatistics.relationshipsDeleted} relations")
        return Pair(queryStatistics.nodesDeleted, queryStatistics.relationshipsDeleted)
    }

    fun getEntitiesByTypeAndQuery(type: String, query: Pair<List<Pair<String, String>>, List<Pair<String, String>>>): List<String> {
        val propertiesFilter =
            if (query.second.isNotEmpty())
                query.second.joinToString(" AND ") {
                    "(n)-[:HAS_VALUE]->({ name: '${it.first}', value: '${it.second}' })"
                }
            else
                ""

        val relationshipsFilter =
            if (query.first.isNotEmpty())
                query.first.joinToString(" AND ") {
                    "(n)-[:HAS_OBJECT]-()-[:${it.first}]->({ id: '${it.second}' })"
                }
            else
                ""

        val matchClause =
            if (type.isEmpty())
                "MATCH (n)"
            else
                "MATCH (n:`$type`)"

        val whereClause =
            if (propertiesFilter.isNotEmpty() || relationshipsFilter.isNotEmpty()) " WHERE "
            else ""

        val finalQuery = """
            $matchClause
            $whereClause
                $propertiesFilter
                ${if (propertiesFilter.isNotEmpty() && relationshipsFilter.isNotEmpty()) " AND " else ""}
                $relationshipsFilter
            RETURN n.id as id
        """

        logger.debug("Issuing search query $finalQuery")
        return session.query(finalQuery, emptyMap<String, Any>(), true)
            .map { it["id"] as String }
    }

    fun getObservedProperty(observerId: String, relationshipType: String): Property? {
        val query = """
            MATCH (p:Property)-[:$relationshipType]->(e:Entity { id: '$observerId' })
            RETURN p
        """.trimIndent()

        logger.debug("Issuing query $query")
        return session.query(query, emptyMap<String, Any>(), true).toMutableList()
            .map { it["p"] as Property }
            .firstOrNull()
    }
}
