package com.egm.stellio.entity.repository

import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.model.toRelationshipTypeName
import com.egm.stellio.entity.util.isDate
import com.egm.stellio.entity.util.isDateTime
import com.egm.stellio.entity.util.isFloat
import com.egm.stellio.entity.util.isTime
import com.egm.stellio.shared.model.NgsiLdPropertyInstance
import org.neo4j.ogm.session.Session
import org.neo4j.ogm.session.SessionFactory
import org.neo4j.ogm.session.event.Event
import org.neo4j.ogm.session.event.EventListenerAdapter
import org.springframework.stereotype.Component
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import javax.annotation.PostConstruct

sealed class SubjectNodeInfo(val id: URI, val label: String)
class EntitySubjectNode(id: URI) : SubjectNodeInfo(id, "Entity")
class AttributeSubjectNode(id: URI) : SubjectNodeInfo(id, "Attribute")

@Component
class Neo4jRepository(
    private val session: Session,
    private val sessionFactory: SessionFactory,
    private val entityRepository: EntityRepository
) {

    fun createPropertyOfSubject(subjectNodeInfo: SubjectNodeInfo, property: Property): String {
        val query =
            """
            MATCH (subject:${subjectNodeInfo.label} { id: ${'$'}subjectId })
            CREATE (subject)-[:HAS_VALUE]->(p:Attribute:Property ${'$'}props)
            RETURN p.id as id
        """

        val parameters = mapOf(
            "props" to property.nodeProperties(),
            "subjectId" to subjectNodeInfo.id.toString()
        )
        return session.query(query, parameters)
            .first()["id"] as String
    }

    fun createRelationshipOfSubject(
        subjectNodeInfo: SubjectNodeInfo,
        relationship: Relationship,
        targetId: URI
    ): URI {
        val relationshipType = relationship.type[0].toRelationshipTypeName()
        val query =
            """
            MATCH (subject:${subjectNodeInfo.label} { id: ${'$'}subjectId }), (target:Entity { id: ${'$'}targetId })
            CREATE (subject)-[:HAS_OBJECT]->(r:Attribute:Relationship:`${relationship.type[0]}` ${'$'}props)-[:$relationshipType]->(target)
            RETURN r.id as id
        """

        val parameters = mapOf(
            "props" to relationship.nodeProperties(),
            "subjectId" to subjectNodeInfo.id.toString(),
            "targetId" to targetId.toString()
        )
        return URI.create(session.query(query, parameters).first()["id"] as String)
    }

    /**
     * Add a spatial property to an entity.
     */
    fun addLocationPropertyToEntity(subjectId: URI, coordinates: Pair<Double, Double>): Int {
        val query =
            """
            MERGE (subject:Entity { id: ${'$'}subjectId })
            ON MATCH SET subject.location = point({x: ${coordinates.first}, y: ${coordinates.second}, crs: 'wgs-84'})
            """

        val parameters = mapOf(
            "subjectId" to subjectId.toString()
        )
        return session.query(query, parameters).queryStatistics().propertiesSet
    }

    fun updateEntityAttribute(entityId: URI, propertyName: String, propertyValue: Any): Int {
        val query =
            """
            MERGE (entity:Entity { id: ${'$'}entityId })-[:HAS_VALUE]->(property:Property { name: ${'$'}propertyName })
            ON MATCH SET property.value = ${escapePropertyValue(propertyValue)},
                         property.modifiedAt = datetime("${Instant.now().atZone(ZoneOffset.UTC)}")
            """.trimIndent()

        val parameters = mapOf(
            "entityId" to entityId.toString(),
            "propertyName" to propertyName
        )
        return session.query(query, parameters).queryStatistics().propertiesSet
    }

    fun updateEntityPropertyInstance(
        subjectNodeInfo: SubjectNodeInfo,
        propertyName: String,
        newPropertyInstance: NgsiLdPropertyInstance
    ): Int {
        /**
         * Update a property instance:
         *
         * 1. Delete old instance
         * 2. Create new instance
         */
        val datasetId = newPropertyInstance.datasetId

        val matchQuery = if (datasetId == null)
            """
            MATCH (entity:${subjectNodeInfo.label} { id: ${'$'}entityId })-[:HAS_VALUE]
            ->(attribute:Property { name: ${'$'}propertyName })
            WHERE NOT EXISTS (attribute.datasetId)
            """.trimIndent()
        else
            """
            MATCH (entity:${subjectNodeInfo.label} { id: ${'$'}entityId })-[:HAS_VALUE]
            ->(attribute:Property { name: ${'$'}propertyName, datasetId: ${'$'}datasetId})
            """.trimIndent()

        var createAttributeQuery =
            """
            WITH DISTINCT entity
            CREATE (entity)-[:HAS_VALUE]->(newAttribute:Attribute:Property ${'$'}props)
            """

        val parameters = mutableMapOf(
            "entityId" to subjectNodeInfo.id.toString(),
            "propertyName" to propertyName,
            "datasetId" to datasetId?.toString(),
            "props" to Property(propertyName, newPropertyInstance).nodeProperties(),
            "propertiesOfProperty" to newPropertyInstance.properties
                .map { Property(it.name, it.instances[0]).nodeProperties() }
        )

        if (newPropertyInstance.properties.isNotEmpty())
            createAttributeQuery = createAttributeQuery.plus(
                """
                WITH newAttribute
                UNWIND ${'$'}propertiesOfProperty AS propertyOfProperty
                CREATE (newAttribute)-[:HAS_VALUE]->(newPropertyOfAttribute:Attribute:Property)
                SET newPropertyOfAttribute = propertyOfProperty
                """
            )

        newPropertyInstance.relationships.filter {
            entityRepository.exists(it.instances[0].objectId.toString()) ?: false
        }.forEachIndexed { index, ngsiLdRelationship ->
            val relationship = Relationship(ngsiLdRelationship.name, ngsiLdRelationship.instances[0])
            parameters["relationshipOfProperty_$index"] = relationship.nodeProperties()
            createAttributeQuery = createAttributeQuery.plus(
                """
                    WITH DISTINCT newAttribute
                    MATCH (target:Entity { id: "${ngsiLdRelationship.instances[0].objectId}" })
                    CREATE (newAttribute)-[:HAS_OBJECT]
                    ->(r:Attribute:Relationship:`${relationship.type[0]}` ${'$'}relationshipOfProperty_$index)
                    -[:${relationship.type[0].toRelationshipTypeName()}]->(target)
                    """
            )
        }

        return session.query(matchQuery + deleteAttributeQuery + createAttributeQuery, parameters)
            .queryStatistics().nodesDeleted
    }

    fun updateEntityModifiedDate(entityId: URI): Int {
        val query =
            """
            MERGE (entity:Entity { id: ${'$'}entityId })
            ON MATCH SET entity.modifiedAt = datetime("${Instant.now().atZone(ZoneOffset.UTC)}")
        """

        return session.query(query, mapOf("entityId" to entityId.toString())).queryStatistics().propertiesSet
    }

    fun hasRelationshipOfType(subjectNodeInfo: SubjectNodeInfo, relationshipType: String): Boolean {
        val query =
            """
            MATCH (a:${subjectNodeInfo.label} { id: ${'$'}attributeId })-[:HAS_OBJECT]->(rel)-[:$relationshipType]->()
            RETURN a.id
            """.trimIndent()

        val parameters = mapOf(
            "attributeId" to subjectNodeInfo.id
        )
        return session.query(query, parameters, true).toList().isNotEmpty()
    }

    fun hasPropertyOfName(subjectNodeInfo: SubjectNodeInfo, propertyName: String): Boolean {
        val query =
            """
            MATCH (a:${subjectNodeInfo.label} { id: ${'$'}attributeId })-[:HAS_VALUE]->(property:Property { name: ${'$'}propertyName })
            RETURN a.id
            """.trimIndent()

        val parameters = mapOf(
            "attributeId" to subjectNodeInfo.id,
            "propertyName" to propertyName
        )
        return session.query(query, parameters, true).toList().isNotEmpty()
    }

    fun hasGeoPropertyOfName(subjectNodeInfo: SubjectNodeInfo, geoPropertyName: String): Boolean {
        val query =
            """
            MATCH (a:${subjectNodeInfo.label} { id: ${'$'}attributeId }) WHERE a.$geoPropertyName IS NOT NULL
            RETURN a.id
            """.trimIndent()

        val parameters = mapOf(
            "attributeId" to subjectNodeInfo.id
        )
        return session.query(query, parameters, true).toList().isNotEmpty()
    }

    fun hasPropertyInstance(subjectNodeInfo: SubjectNodeInfo, propertyName: String, datasetId: URI? = null): Boolean {
        val query = if (datasetId == null)
            """
            MATCH (a:${subjectNodeInfo.label} { id: ${'$'}attributeId })-[:HAS_VALUE]->(property:Property { name: ${'$'}propertyName })
            WHERE NOT EXISTS (property.datasetId)
            RETURN a.id
            """.trimIndent()
        else
            """
            MATCH (a:${subjectNodeInfo.label} { id: ${'$'}attributeId })-[:HAS_VALUE]->(property:Property { name: ${'$'}propertyName, datasetId: ${'$'}datasetId })
            RETURN a.id
            """.trimIndent()

        val parameters = mapOf(
            "attributeId" to subjectNodeInfo.id.toString(),
            "propertyName" to propertyName,
            "datasetId" to datasetId?.toString()
        )

        return session.query(query, parameters, true).toList().isNotEmpty()
    }

    fun hasRelationshipInstance(
        subjectNodeInfo: SubjectNodeInfo,
        relationshipType: String,
        datasetId: URI? = null
    ): Boolean {
        val query = if (datasetId == null)
            """
            MATCH (a:${subjectNodeInfo.label} { id: ${'$'}attributeId })-[:HAS_OBJECT]->(rel:Relationship)-[:$relationshipType]->()
            WHERE NOT EXISTS (rel.datasetId)
            RETURN a.id
            """.trimIndent()
        else
            """
            MATCH (a:${subjectNodeInfo.label} { id: ${'$'}attributeId })-[:HAS_OBJECT]->(rel:Relationship { datasetId: ${'$'}datasetId })-[:$relationshipType]->()
            RETURN a.id
            """.trimIndent()

        val parameters = mapOf(
            "attributeId" to subjectNodeInfo.id.toString(),
            "datasetId" to datasetId?.toString()
        )
        return session.query(query, parameters, true).toList().isNotEmpty()
    }

    fun updateRelationshipTargetOfAttribute(
        attributeId: URI,
        relationshipType: String,
        oldRelationshipObjectId: URI,
        newRelationshipObjectId: URI
    ): Int {
        val relationshipTypeQuery =
            """
            MATCH (a:Attribute { id: ${'$'}attributeId })-[v:$relationshipType]->(e:Entity { id: ${'$'}oldRelationshipObjectId }),
                  (target:Entity { id: ${'$'}newRelationshipObjectId })
            DETACH DELETE v
            MERGE (a)-[:$relationshipType]->(target)
            """.trimIndent()

        val parameters = mapOf(
            "attributeId" to attributeId.toString(),
            "oldRelationshipObjectId" to oldRelationshipObjectId.toString(),
            "newRelationshipObjectId" to newRelationshipObjectId.toString()
        )
        return session.query(relationshipTypeQuery, parameters).queryStatistics().nodesDeleted
    }

    fun updateLocationPropertyOfEntity(entityId: URI, coordinates: Pair<Double, Double>): Int {
        val query =
            """
            MERGE (entity:Entity { id: "$entityId" })
            ON MATCH SET entity.location = point({x: ${coordinates.first}, y: ${coordinates.second}, crs: 'wgs-84'})
            """
        return session.query(query, emptyMap<String, String>()).queryStatistics().propertiesSet
    }

    fun deleteEntity(entityId: URI): Pair<Int, Int> {
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
        val query =
            """
            MATCH (n:Entity { id: ${'$'}entityId }) 
            OPTIONAL MATCH (n)-[:HAS_VALUE]->(prop:Property)
            WITH n, prop
            OPTIONAL MATCH (prop)-[:HAS_OBJECT]->(relOfProp:Relationship)
            WITH n, prop, relOfProp
            OPTIONAL MATCH (prop)-[:HAS_VALUE]->(propOfProp:Property)
            WITH n, prop, relOfProp, propOfProp
            OPTIONAL MATCH (n)-[:HAS_OBJECT]->(rel:Relationship)
            WITH n, prop, relOfProp, propOfProp, rel
            OPTIONAL MATCH (rel)-[:HAS_VALUE]->(propOfRel:Property)
            WITH n, prop, relOfProp, propOfProp, rel, propOfRel
            OPTIONAL MATCH (rel)-[:HAS_OBJECT]->(relOfRel:Relationship)
            DETACH DELETE n, prop, relOfProp, propOfProp, rel, propOfRel, relOfRel
            """.trimIndent()

        val parameters = mapOf(
            "entityId" to entityId.toString()
        )
        val queryStatistics = session.query(query, parameters).queryStatistics()
        return Pair(queryStatistics.nodesDeleted, queryStatistics.relationshipsDeleted)
    }

    fun deleteEntityAttributes(entityId: URI): Pair<Int, Int> {
        /**
         * Delete :
         *
         * 1. the properties
         * 2. the properties of properties
         * 3. the relationships of properties
         * 4. the relationships
         * 5. the properties of relationships
         * 6. the relationships of relationships
         */
        val query =
            """
            MATCH (n:Entity { id: ${'$'}entityId }) 
            OPTIONAL MATCH (n)-[:HAS_VALUE]->(prop:Property)
            WITH n, prop
            OPTIONAL MATCH (prop)-[:HAS_OBJECT]->(relOfProp:Relationship)
            WITH n, prop, relOfProp
            OPTIONAL MATCH (prop)-[:HAS_VALUE]->(propOfProp:Property)
            WITH n, prop, relOfProp, propOfProp
            OPTIONAL MATCH (n)-[:HAS_OBJECT]->(rel:Relationship)
            WITH n, prop, relOfProp, propOfProp, rel
            OPTIONAL MATCH (rel)-[:HAS_VALUE]->(propOfRel:Property)
            WITH n, prop, relOfProp, propOfProp, rel, propOfRel
            OPTIONAL MATCH (rel)-[:HAS_OBJECT]->(relOfRel:Relationship)
            DETACH DELETE prop, relOfProp, propOfProp, rel, propOfRel, relOfRel
            """.trimIndent()

        val parameters = mapOf(
            "entityId" to entityId.toString()
        )
        val queryStatistics = session.query(query, parameters).queryStatistics()
        return Pair(queryStatistics.nodesDeleted, queryStatistics.relationshipsDeleted)
    }

    fun deleteEntityProperty(
        subjectNodeInfo: SubjectNodeInfo,
        propertyName: String,
        datasetId: URI? = null,
        deleteAll: Boolean = false
    ): Int {
        /**
         * Delete :
         *
         * 1. the property
         * 2. the properties of the property
         * 3. the relationships of the property
         */
        val matchQuery = if (deleteAll)
            """
            MATCH (entity:${subjectNodeInfo.label} { id: ${'$'}entityId })-[:HAS_VALUE]->(attribute:Property { name: ${'$'}propertyName })
            """.trimIndent()
        else if (datasetId == null)
            """
            MATCH (entity:${subjectNodeInfo.label} { id: ${'$'}entityId })-[:HAS_VALUE]->(attribute:Property { name: ${'$'}propertyName })
            WHERE NOT EXISTS (attribute.datasetId)
            """.trimIndent()
        else
            """
            MATCH (entity:${subjectNodeInfo.label} { id: ${'$'}entityId })-[:HAS_VALUE]->(attribute:Property { name: ${'$'}propertyName, datasetId: ${'$'}datasetId})
            """.trimIndent()

        val parameters = mapOf(
            "entityId" to subjectNodeInfo.id.toString(),
            "propertyName" to propertyName,
            "datasetId" to datasetId?.toString()
        )

        return session.query(matchQuery + deleteAttributeQuery, parameters).queryStatistics().nodesDeleted
    }

    fun deleteEntityRelationship(
        subjectNodeInfo: SubjectNodeInfo,
        relationshipType: String,
        datasetId: URI? = null,
        deleteAll: Boolean = false
    ): Int {
        /**
         * Delete :
         *
         * 1. the relationship
         * 2. the properties of the relationship
         * 3. the relationships of the relationship
         */
        val matchQuery = if (deleteAll)
            """
            MATCH (entity:${subjectNodeInfo.label} { id: ${'$'}entityId })-[:HAS_OBJECT]->(attribute:Relationship)-[:$relationshipType]->()
            """.trimIndent()
        else if (datasetId == null)
            """
            MATCH (entity:${subjectNodeInfo.label} { id: ${'$'}entityId })-[:HAS_OBJECT]->(attribute:Relationship)-[:$relationshipType]->()
            WHERE NOT EXISTS (attribute.datasetId)
            """.trimIndent()
        else
            """
            MATCH (entity:${subjectNodeInfo.label} { id: ${'$'}entityId })-[:HAS_OBJECT]->(attribute:Relationship { datasetId: ${'$'}datasetId})-[:$relationshipType]->()
            """.trimIndent()

        val parameters = mapOf(
            "entityId" to subjectNodeInfo.id.toString(),
            "datasetId" to datasetId?.toString()
        )

        return session.query(matchQuery + deleteAttributeQuery, parameters).queryStatistics().nodesDeleted
    }

    fun getEntitiesByTypeAndQuery(
        type: String,
        query: Pair<List<Triple<String, String, String>>, List<Triple<String, String, String>>>
    ): List<URI> {
        val propertiesFilter =
            if (query.second.isNotEmpty())
                query.second.joinToString(" AND ") {
                    val comparableValue = when {
                        it.third.isFloat() -> "toFloat('${it.third}')"
                        it.third.isDateTime() -> "datetime('${it.third}')"
                        it.third.isDate() -> "date('${it.third}')"
                        it.third.isTime() -> "localtime('${it.third}')"
                        else -> "'${it.third}'"
                    }
                    """
                   EXISTS {
                       MATCH (n)-[:HAS_VALUE]->(p:Property)
                       WHERE p.name = '${it.first}' 
                       AND p.value ${it.second} $comparableValue
                   }
                    """.trimIndent()
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
                "MATCH (n:Entity)"
            else
                "MATCH (n:`$type`)"

        val whereClause =
            if (propertiesFilter.isNotEmpty() || relationshipsFilter.isNotEmpty()) " WHERE "
            else ""

        val finalQuery =
            """
            $matchClause
            $whereClause
                $propertiesFilter
                ${if (propertiesFilter.isNotEmpty() && relationshipsFilter.isNotEmpty()) " AND " else ""}
                $relationshipsFilter
            RETURN n.id as id
            """

        return session.query(finalQuery, emptyMap<String, Any>(), true)
            .map { URI.create(it["id"] as String) }
    }

    fun getObservingSensorEntity(observerId: URI, propertyName: String, measureName: String): Entity? {
        // definitely not bullet proof since we are looking for a property whose name ends with the property name
        // received from the Kafka observations topic (but in this case, we miss the @context to do a proper expansion)
        // TODO : this will have to be resolved with a clean provisioning architecture
        val query =
            """
            MATCH (e:Entity) 
            WHERE e.id = '$observerId' 
            RETURN e 
            UNION 
            MATCH (m:Property)-[:HAS_OBJECT]-()-[:observedBy]->(e:Entity)-[:HAS_VALUE]->(p:Property) 
            WHERE m.name ENDS WITH '$measureName' 
            AND p.name = '$propertyName' 
            AND toLower(p.value) = toLower('$observerId') 
            RETURN e
            UNION
            MATCH (m:Property)-[:HAS_OBJECT]-()-[:observedBy]->
            (e:Entity)-[:HAS_OBJECT]-()-[:isContainedIn]->(device:Entity)-[:HAS_VALUE]->(deviceProp:Property)
            WHERE m.name ENDS WITH '$measureName' 
            AND deviceProp.name = '$propertyName' 
            AND toLower(deviceProp.value) = toLower('$observerId') 
            RETURN e
            """.trimIndent()

        return session.query(query, emptyMap<String, Any>(), true).toMutableList()
            .map { it["e"] as Entity }
            .firstOrNull()
    }

    fun getObservedProperty(observerId: URI, relationshipType: String): Property? {
        val query =
            """
            MATCH (p:Property)-[:HAS_OBJECT]->(r:Relationship)-[:$relationshipType]->(e:Entity { id: '$observerId' })
            RETURN p
            """.trimIndent()

        return session.query(query, emptyMap<String, Any>(), true).toMutableList()
            .map { it["p"] as Property }
            .firstOrNull()
    }

    fun getEntityByProperty(property: Property): Entity {
        val query =
            """
            MATCH (n:Entity)-[:HAS_VALUE]->(p:Property { id: '${property.id}' })
            RETURN n
            """.trimIndent()

        return session.query(query, emptyMap<String, Any>(), true).toMutableList()
            .map { it["n"] as Entity }
            .first()
    }

    fun filterExistingEntitiesAsIds(entitiesIds: List<URI>): List<URI> {
        if (entitiesIds.isEmpty()) {
            return emptyList()
        }

        val query = "MATCH (entity:Entity) WHERE entity.id IN \$entitiesIds RETURN entity.id as id"

        return session.query(query, mapOf("entitiesIds" to entitiesIds.map { it.toString() }), true)
            .map { URI.create(it["id"] as String) }
    }

    fun getPropertyOfSubject(subjectId: URI, propertyName: String, datasetId: URI? = null): Property {
        val query = if (datasetId == null)
            """
            MATCH ({ id: '$subjectId' })-[:HAS_VALUE]->(p:Property { name: "$propertyName" })
            WHERE NOT EXISTS (p.datasetId)
            RETURN p
            """.trimIndent()
        else
            """
            MATCH ({ id: '$subjectId' })-[:HAS_VALUE]->(p:Property { name: "$propertyName", datasetId: "$datasetId" })
            RETURN p
            """.trimIndent()

        return session.query(query, emptyMap<String, Any>(), true).toMutableList()
            .map { it["p"] as Property }
            .first()
    }

    fun getRelationshipOfSubject(subjectId: URI, relationshipType: String): Relationship {
        val query =
            """
            MATCH ({ id: '$subjectId' })-[:HAS_OBJECT]->(r:Relationship)-[:$relationshipType]->()
            RETURN r
            """.trimIndent()

        return session.query(query, emptyMap<String, Any>(), true).toMutableList()
            .map { it["r"] as Relationship }
            .first()
    }

    fun getRelationshipTargetOfSubject(subjectId: URI, relationshipType: String): Entity? {
        val query =
            """
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

    private val deleteAttributeQuery =
        """
        OPTIONAL MATCH (attribute)-[:HAS_VALUE]->(propOfAttribute:Property)
        WITH entity, attribute, propOfAttribute
        OPTIONAL MATCH (attribute)-[:HAS_OBJECT]->(relOfAttribute:Relationship)
        DETACH DELETE attribute, propOfAttribute, relOfAttribute
        """.trimIndent()

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
                entity.modifiedAt = Instant.now().atZone(ZoneOffset.UTC)
            }
            is Property -> {
                val property = event.getObject() as Property
                property.modifiedAt = Instant.now().atZone(ZoneOffset.UTC)
            }
            is Relationship -> {
                val relationship = event.getObject() as Relationship
                relationship.modifiedAt = Instant.now().atZone(ZoneOffset.UTC)
            }
        }
    }
}
