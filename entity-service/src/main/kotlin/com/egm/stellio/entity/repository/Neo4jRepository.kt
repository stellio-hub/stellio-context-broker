package com.egm.stellio.entity.repository

import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.util.isFloat
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import org.neo4j.ogm.session.Session
import org.neo4j.ogm.session.SessionFactory
import org.neo4j.ogm.session.event.Event
import org.neo4j.ogm.session.event.EventListenerAdapter
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.time.OffsetDateTime
import javax.annotation.PostConstruct

@Component
class Neo4jRepository(
    private val session: Session,
    private val sessionFactory: SessionFactory
) {
    private val logger = LoggerFactory.getLogger(javaClass)

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
            ON MATCH SET property.value = ${escapePropertyValue(propertyValue)},
                         property.modifiedAt = "${OffsetDateTime.now()}"
        """.trimIndent()

        return session.query(query, emptyMap<String, String>()).queryStatistics().propertiesSet
    }

    fun hasRelationshipOfType(attributeId: String, relationshipType: String): Boolean {
        val query = """
            MATCH (a { id: '$attributeId' })-[:HAS_OBJECT]->(rel)-[:$relationshipType]->()
            RETURN a.id
        """.trimIndent()

        return session.query(query, emptyMap<String, String>(), true).toList().isNotEmpty()
    }

    fun hasPropertyOfName(attributeId: String, propertyName: String): Boolean {
        val query = """
            MATCH (a { id: '$attributeId' })-[:HAS_VALUE]->(property:Property { name: "$propertyName" })
            RETURN a.id
        """.trimIndent()

        return session.query(query, emptyMap<String, String>(), true).toList().isNotEmpty()
    }

    fun hasGeoPropertyOfName(attributeId: String, geoPropertyName: String): Boolean {
        val query = """
            MATCH (a { id: '$attributeId' }) WHERE a.$geoPropertyName IS NOT NULL
            RETURN a.id
        """.trimIndent()

        return session.query(query, emptyMap<String, String>(), true).toList().isNotEmpty()
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
    fun updateRelationshipTargetOfAttribute(
        attributeId: String,
        relationshipType: String,
        oldRelationshipObjectId: String,
        newRelationshipObjectId: String
    ): Pair<Int, Int> {
        val hasObjectQuery = """
            MATCH (a:Attribute { id: "$attributeId" })-[v:HAS_OBJECT]->(e:Entity { id: '$oldRelationshipObjectId' }),
                  (target:Entity { id: "$newRelationshipObjectId" })
            DETACH DELETE v
            MERGE (a)-[:HAS_OBJECT]->(target)
        """.trimIndent()

        val relationshipTypeQuery = """
            MATCH (a:Attribute { id: "$attributeId" })-[v:$relationshipType]->(e:Entity { id: '$oldRelationshipObjectId' }),
                  (target:Entity { id: "$newRelationshipObjectId" })
            DETACH DELETE v
            MERGE (a)-[:$relationshipType]->(target)
        """.trimIndent()

        val objectQueryStatistics = session.query(hasObjectQuery, emptyMap<String, String>()).queryStatistics().nodesDeleted
        val relationshipTypeQueryStatistics = session.query(relationshipTypeQuery, emptyMap<String, String>()).queryStatistics().nodesDeleted

        return Pair(objectQueryStatistics, relationshipTypeQueryStatistics)
    }

    @Transactional
    fun updateLocationPropertyOfEntity(entityId: String, coordinates: Pair<Double, Double>): Int {
        val query = """
            MERGE (entity:Entity { id: "$entityId" })
            ON MATCH SET entity.location = point({x: ${coordinates.first}, y: ${coordinates.second}, crs: 'wgs-84'})
        """
        return session.query(query, emptyMap<String, String>()).queryStatistics().propertiesSet
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
         * 6. the properties of relationships
         * 7. the relationships of relationships
         */
        val query = """
            MATCH (n:Entity { id: '$entityId' }) 
            OPTIONAL MATCH (n)-[:HAS_VALUE]->(prop)
            OPTIONAL MATCH (prop)-[:HAS_OBJECT]->(relOfProp)
            OPTIONAL MATCH (prop)-[:HAS_VALUE]->(propOfProp)
            OPTIONAL MATCH (n)-[:HAS_OBJECT]->(rel)
            OPTIONAL MATCH (rel)-[:HAS_VALUE]->(propOfRel)
            OPTIONAL MATCH (rel)-[:HAS_OBJECT]->(relOfRel:Relationship)
            DETACH DELETE n,prop,relOfProp,propOfProp,rel,propOfRel,relOfRel
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

    fun getObservingSensorEntity(observerId: String, propertyName: String, measureName: String): Entity? {
        // definitely not bullet proof since we are looking for a property whose name ends with the property name
        // received from the Kafka observations topic (but in this case, we miss the @context to do a proper expansion)
        // TODO : this will have to be resolved with a clean provisioning architecture
        val query = """
            MATCH (e:Entity)
            WHERE e.id = '$observerId'
            RETURN e
            UNION ALL
            MATCH (m:Property)-[:HAS_OBJECT]-()-[:OBSERVED_BY]->(e:Entity)-[:HAS_VALUE]->(p:Property)
            WHERE m.name ENDS WITH '$measureName' AND p.name = '$propertyName' AND p.value = '$observerId'
            RETURN e
        """.trimIndent()

        logger.debug("Issuing query $query")
        return session.query(query, emptyMap<String, Any>(), true).toMutableList()
            .map { it["e"] as Entity }
            .firstOrNull()
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

    fun getPropertyOfSubject(subjectId: String, propertyName: String): Property {
        val query = """
            MATCH ({ id: '$subjectId' })-[:HAS_VALUE]->(p:Property { name: "$propertyName" })
            RETURN p
        """.trimIndent()

        return session.query(query, emptyMap<String, Any>(), true).toMutableList()
            .map { it["p"] as Property }
            .first()
    }

    fun getRelationshipOfSubject(subjectId: String, relationshipType: String): Relationship {
        val query = """
            MATCH ({ id: '$subjectId' })-[:HAS_OBJECT]->(r:Relationship)-[:$relationshipType]->()
            RETURN r
        """.trimIndent()

        return session.query(query, emptyMap<String, Any>(), true).toMutableList()
            .map { it["r"] as Relationship }
            .first()
    }

    fun getRelationshipTargetOfSubject(subjectId: String, relationshipType: String): Entity? {
        val query = """
            MATCH ({ id: '$subjectId' })-[:HAS_OBJECT]->(r:Relationship)-[:$relationshipType]->(e: Entity)
            RETURN e
        """.trimIndent()
        return session.query(query, emptyMap<String, Any>(), true).toMutableList()
            .map { it["e"] as Entity }
            .firstOrNull()
    }

    private fun escapePropertyValue(value: Any): Any {
        return when (value) {
            is String -> "\"$value\""
            else -> value
        }
    }

    @PostConstruct
    fun addEventListenerToSessionFactory() {
        val eventListener = PreSaveEventListener()
        sessionFactory.register(eventListener)
    }
}

class PreSaveEventListener : EventListenerAdapter() {
    override fun onPreSave(event: Event) {
        when (event.getObject()) {
            is Entity -> {
                val entity = event.getObject() as Entity
                entity.modifiedAt = OffsetDateTime.now()
            }
            is Property -> {
                val property = event.getObject() as Property
                property.modifiedAt = OffsetDateTime.now()
            }
            is Relationship -> {
                val relationship = event.getObject() as Relationship
                relationship.modifiedAt = OffsetDateTime.now()
            }
        }
    }
}
