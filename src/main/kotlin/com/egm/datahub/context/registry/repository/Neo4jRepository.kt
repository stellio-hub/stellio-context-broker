package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.model.Entity
import com.egm.datahub.context.registry.model.Property
import com.egm.datahub.context.registry.util.isFloat
import com.egm.datahub.context.registry.util.extractShortTypeFromExpanded
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
     * Create the concrete relationship from a relationship to an entity.
     *
     * It is used for :
     *   - relationships of relationships
     *   - relationships of properties
     */
    @Transactional
    fun createRelationshipToEntity(relationshipId: String, relationshipType: String, entityId: String): Int {
        val query = """
            MATCH (subject:Attribute { id: "$relationshipId" }), (target:Entity { id: "$entityId" })
            MERGE (subject)-[:$relationshipType]->(target)
            MERGE (subject)-[:HAS_OBJECT]->(target)
        """

        return session.query(query, emptyMap<String, String>()).queryStatistics().relationshipsCreated
    }

    /**
     * Add a spatial property to an entity.
     */
    @Transactional
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

    fun hasRelationshipOfType(entityId: String, relationshipType: String): Boolean {
        val query = """
            MATCH (n:Entity { id: '$entityId' })-[:HAS_OBJECT]->(rel)-[:$relationshipType]->()
            RETURN n.id
        """.trimIndent()

        return session.query(query, emptyMap<String, String>()).toList().isNotEmpty()
    }

    fun hasPropertyOfName(entityId: String, propertyName: String): Boolean {
        val query = """
            MATCH (n:Entity { id: '$entityId' })-[:HAS_VALUE]->(property:Property { name: "$propertyName" })
            RETURN n.id
        """.trimIndent()

        return session.query(query, emptyMap<String, String>()).toList().isNotEmpty()
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

    @Transactional
    fun deleteEntity(entityId: String): Pair<Int, Int> {
        /**
         * Delete :
         *
         * 1. the entity node
         * 2. the properties
         * 3. the properties of properties
         * 4. the relationships of properties
         * 5. the relationships
         * 6. the relationships of relationships
         */
        val query = """
            MATCH (n:Entity { id: '$entityId' }) 
            OPTIONAL MATCH (n)-[:HAS_VALUE]->(prop)
            OPTIONAL MATCH (prop)-[:HAS_OBJECT]->(relOfProp)
            OPTIONAL MATCH (prop)-[:HAS_VALUE]->(propOfProp)
            OPTIONAL MATCH (n)-[:HAS_OBJECT]->(rel)
            OPTIONAL MATCH (rel)-[:HAS_OBJECT]->(relOfRel)
            DETACH DELETE n,prop,relOfProp,propOfProp,rel,relOfRel
        """.trimIndent()

        val queryStatistics = session.query(query, emptyMap<String, Any>()).queryStatistics()
        logger.debug("Deleted entity $entityId : deleted ${queryStatistics.nodesDeleted} nodes, ${queryStatistics.relationshipsDeleted} relations")
        return Pair(queryStatistics.nodesDeleted, queryStatistics.relationshipsDeleted)
    }

    fun getEntitiesByTypeAndQuery(type: String, query: Pair<List<Triple<String, String, String>>, List<Triple<String, String, String>>>): List<String> {
        val propertiesFilter =
            if (query.second.isNotEmpty())
                query.second.joinToString(" AND ") {
                    if (it.third.isFloat())
                        "(n)-[:HAS_VALUE]->(${it.first.extractShortTypeFromExpanded()}: Property { name: '${it.first}'}) and ${it.first.extractShortTypeFromExpanded()}.value ${it.second} toFloat('${it.third}')"
                    else
                        "(n)-[:HAS_VALUE]->(${it.first.extractShortTypeFromExpanded()}: Property { name: '${it.first}'}) and ${it.first.extractShortTypeFromExpanded()}.value ${it.second} '${it.third}'"
                }
            else
                ""

        val propertiesVariables =
            if (query.second.isNotEmpty())
                query.second.joinToString(", ") {
                    "(${it.first.extractShortTypeFromExpanded()})"
                }
            else
                ""

        val relationshipsFilter =
            if (query.first.isNotEmpty())
                query.first.joinToString(" AND ") {
                    "(n)-[:HAS_OBJECT]-()-[:${it.first}]->({ id: '${it.third}' })"
                }
            else
                ""

        val matchClause =
            if (type.isEmpty())
                "MATCH (n)"
            else {
                if (propertiesVariables.isEmpty())
                    "MATCH (n:`$type`)"
                else
                    "MATCH (n:`$type`), $propertiesVariables"
            }
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
            MATCH (p:Property)-[:HAS_OBJECT]->(r:Relationship)-[:$relationshipType]->(e:Entity { id: '$observerId' })
            RETURN p
        """.trimIndent()

        logger.debug("Issuing query $query")
        return session.query(query, emptyMap<String, Any>(), true).toMutableList()
            .map { it["p"] as Property }
            .firstOrNull()
    }

    fun getEntityByProperty(property: Property): Entity {
        val query = """
            MATCH (n:Entity)-[:HAS_VALUE]->(p:Property { id: '${property.id}' })
            RETURN n
        """.trimIndent()

        logger.debug("Issuing query $query")
        return session.query(query, emptyMap<String, Any>(), true).toMutableList()
            .map { it["n"] as Entity }
            .first()
    }
}