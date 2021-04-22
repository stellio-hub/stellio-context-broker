package com.egm.stellio.entity.repository

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.model.toRelationshipTypeName
import com.egm.stellio.entity.util.*
import com.egm.stellio.shared.model.NgsiLdGeoPropertyInstance
import com.egm.stellio.shared.model.NgsiLdGeoPropertyInstance.Companion.toWktFormat
import com.egm.stellio.shared.model.NgsiLdPropertyInstance
import com.egm.stellio.shared.util.toListOfString
import com.egm.stellio.shared.util.toUri
import org.neo4j.ogm.session.Session
import org.neo4j.ogm.session.SessionFactory
import org.neo4j.ogm.session.event.Event
import org.neo4j.ogm.session.event.EventListenerAdapter
import org.springframework.stereotype.Component
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.util.regex.Pattern
import javax.annotation.PostConstruct

sealed class SubjectNodeInfo(val id: URI, val label: String)
class EntitySubjectNode(id: URI) : SubjectNodeInfo(id, "Entity")
class AttributeSubjectNode(id: URI) : SubjectNodeInfo(id, "Attribute")

@Component
class Neo4jRepository(
    private val session: Session,
    private val sessionFactory: SessionFactory
) {

    fun mergePartialWithNormalEntity(id: URI): Int {
        val query =
            """
            MATCH (e:Entity { id: ${'$'}id }), (pe:PartialEntity { id: ${'$'}id })
            WITH head(collect([e,pe])) as nodes
            CALL apoc.refactor.mergeNodes(nodes, { properties:"discard" })
            YIELD node
            REMOVE node:PartialEntity
            RETURN node
        """

        val parameters = mapOf(
            "id" to id
        )

        return session.query(query, parameters).queryStatistics().nodesDeleted
    }

    fun createPropertyOfSubject(subjectNodeInfo: SubjectNodeInfo, property: Property): Boolean {
        val query =
            """
            MATCH (subject:${subjectNodeInfo.label} { id: ${'$'}subjectId })
            CREATE (subject)-[:HAS_VALUE]->(p:Attribute:Property ${'$'}props)
            RETURN p.id as id
        """

        val parameters = mapOf(
            "props" to property.nodeProperties(),
            "subjectId" to subjectNodeInfo.id
        )
        return session.query(query, parameters).queryStatistics().containsUpdates()
    }

    fun createRelationshipOfSubject(
        subjectNodeInfo: SubjectNodeInfo,
        relationship: Relationship,
        targetId: URI
    ): Boolean {
        val relationshipType = relationship.type[0].toRelationshipTypeName()
        val query =
            """
            MATCH (subject:${subjectNodeInfo.label} { id: ${'$'}subjectId })
            MERGE (target { id: ${'$'}targetId })
            ON CREATE SET target:PartialEntity
            CREATE (subject)-[:HAS_OBJECT]->
                (r:Attribute:Relationship:`${relationship.type[0]}` ${'$'}props)-[:$relationshipType]->(target)
            RETURN r.id as id
        """

        val parameters = mapOf(
            "props" to relationship.nodeProperties(),
            "subjectId" to subjectNodeInfo.id,
            "targetId" to targetId
        )

        return session.query(query, parameters).queryStatistics().containsUpdates()
    }

    /**
     * Add a spatial property to an entity.
     */
    fun addLocationPropertyToEntity(subjectId: URI, geoProperty: NgsiLdGeoPropertyInstance): Int {
        val query =
            """
            MERGE (subject:Entity { id: ${'$'}subjectId })
            ON MATCH SET subject.location = "${toWktFormat(geoProperty.geoPropertyType, geoProperty.coordinates)}"
            """

        val parameters = mapOf(
            "subjectId" to subjectId
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
            "entityId" to subjectNodeInfo.id,
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

        newPropertyInstance.relationships.forEachIndexed { index, ngsiLdRelationship ->
            val relationship = Relationship(ngsiLdRelationship.name, ngsiLdRelationship.instances[0])
            parameters["relationshipOfProperty_$index"] = relationship.nodeProperties()
            createAttributeQuery = createAttributeQuery.plus(
                """
                    WITH DISTINCT newAttribute
                    MERGE (target { id: "${ngsiLdRelationship.instances[0].objectId}" })
                    ON CREATE SET target:PartialEntity 
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

        return session.query(query, mapOf("entityId" to entityId)).queryStatistics().propertiesSet
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
            "attributeId" to subjectNodeInfo.id,
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
            "attributeId" to subjectNodeInfo.id,
            "datasetId" to datasetId?.toString()
        )
        return session.query(query, parameters, true).toList().isNotEmpty()
    }

    fun updateRelationshipTargetOfSubject(
        subjectId: URI,
        relationshipType: String,
        newRelationshipObjectId: URI
    ): Boolean {
        val relationshipTypeQuery =
            """
            MATCH ({ id: ${'$'}subjectId })-[:HAS_OBJECT]->(r:Relationship)-[v:$relationshipType]->()
            MERGE (target { id: ${'$'}newRelationshipObjectId })
            ON CREATE SET target:PartialEntity
            DETACH DELETE v
            MERGE (r)-[:$relationshipType]->(target)
            """.trimIndent()

        val parameters = mapOf(
            "subjectId" to subjectId,
            "newRelationshipObjectId" to newRelationshipObjectId
        )

        return session.query(relationshipTypeQuery, parameters).queryStatistics().containsUpdates()
    }

    fun updateTargetOfRelationship(
        attributeId: URI,
        relationshipType: String,
        oldRelationshipObjectId: URI,
        newRelationshipObjectId: URI
    ): Int {
        val relationshipTypeQuery =
            """
            MATCH (a:Attribute { id: ${'$'}attributeId })-[v:$relationshipType]
                ->(e { id: ${'$'}oldRelationshipObjectId })
            MERGE (target { id: ${'$'}newRelationshipObjectId })
            ON CREATE SET target:PartialEntity
            DETACH DELETE v
            MERGE (a)-[:$relationshipType]->(target)
            """.trimIndent()

        val parameters = mapOf(
            "attributeId" to attributeId,
            "oldRelationshipObjectId" to oldRelationshipObjectId,
            "newRelationshipObjectId" to newRelationshipObjectId
        )
        return session.query(relationshipTypeQuery, parameters).queryStatistics().nodesDeleted
    }

    fun updateLocationPropertyOfEntity(entityId: URI, geoProperty: NgsiLdGeoPropertyInstance): Int {
        val query =
            """
            MERGE (entity:Entity { id: "$entityId" })
            ON MATCH SET entity.location = "${toWktFormat(geoProperty.geoPropertyType, geoProperty.coordinates)}"
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
         * 8. the incoming relationships (incl. authorizations ones)
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
            WITH n, prop, relOfProp, propOfProp, rel, propOfRel, relOfRel            
            OPTIONAL MATCH (inRel:Relationship)-[]->(n)
            DETACH DELETE n, prop, relOfProp, propOfProp, rel, propOfRel, relOfRel, inRel
            """.trimIndent()

        val parameters = mapOf(
            "entityId" to entityId
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
            "entityId" to entityId
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
            "entityId" to subjectNodeInfo.id,
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
            "entityId" to subjectNodeInfo.id,
            "datasetId" to datasetId?.toString()
        )

        return session.query(matchQuery + deleteAttributeQuery, parameters).queryStatistics().nodesDeleted
    }

    fun getEntityTypeAttributesInformation(expandedType: String): Map<String, Any> {
        // The match on geoProperties is specific since they are not fully supported
        // (currently we only support location that is stored among the entity node attributes)
        val query =
            """
                MATCH (entity:Entity:`$expandedType`)
                WITH count(entity) as entityCount  
                OPTIONAL MATCH (entityWithLocation:Entity:`$expandedType`) WHERE entityWithLocation.location IS NOT NULL
                WITH entityCount, count(entityWithLocation) as entityWithLocationCount
                MATCH (entity:Entity:`$expandedType`)
                OPTIONAL MATCH (entity)-[:HAS_VALUE]->(property:Property)
                OPTIONAL MATCH (entity)-[:HAS_OBJECT]->(rel:Relationship)
                RETURN entityCount, entityWithLocationCount, collect(distinct property.name) as propertyNames, 
                    reduce(output = [], r IN collect(distinct labels(rel)) | output + r) as relationshipNames
            """.trimIndent()

        val result = session.query(query, emptyMap<String, Any>(), true).toList()
        if (result.isEmpty())
            return emptyMap()

        val entityCount = (result.first()["entityCount"] as Long).toInt()
        val entityWithLocationCount = (result.first()["entityWithLocationCount"] as Long).toInt()
        return mapOf(
            "properties" to (result.first()["propertyNames"] as Array<Any>).toSet(),
            "relationships" to (result.first()["relationshipNames"] as Array<Any>)
                .filter { it !in listOf("Attribute", "Relationship") }.toSet(),
            "geoProperties" to
                if (entityWithLocationCount > 0) setOf("https://uri.etsi.org/ngsi-ld/location") else emptySet(),
            "entityCount" to entityCount
        )
    }

    fun getEntityTypes(): List<Map<String, Any>> {
        // The match on geoProperties is specific since they are not fully supported
        // (currently we only support location that is stored among the entity node attributes)
        val query =
            """
                MATCH (entity:Entity)
                OPTIONAL MATCH (entity)-[:HAS_VALUE]->(property:Property)
                OPTIONAL MATCH (entity)-[:HAS_OBJECT]->(rel:Relationship)
                RETURN labels(entity) as entityType, collect(distinct property.name) as propertyNames, 
                    reduce(output = [], r IN collect(distinct labels(rel)) | output + r) as relationshipNames,
                    count(entity.location) as entityWithLocationCount
            """.trimIndent()

        val result = session.query(query, emptyMap<String, Any>(), true).toList()
        return result.map { rowResult ->
            val entityWithLocationCount = (rowResult["entityWithLocationCount"] as Long).toInt()
            val entityTypes = (rowResult["entityType"] as Array<String>)
                .filter { !authorizationEntitiesTypes.plus("Entity").contains(it) }
            entityTypes.map { entityType ->
                mapOf(
                    "entityType" to entityType,
                    "properties" to (rowResult["propertyNames"] as Array<Any>).toSet(),
                    "relationships" to (rowResult["relationshipNames"] as Array<Any>)
                        .filter { it !in listOf("Attribute", "Relationship") }.toSet(),
                    "geoProperties" to
                        if (entityWithLocationCount > 0) setOf("https://uri.etsi.org/ngsi-ld/location") else emptySet()
                )
            }
        }.flatten()
    }

    fun getEntityTypesNames(): List<String> {
        val query =
            """
                MATCH (entity:Entity)
                RETURN DISTINCT(labels(entity)) as entityType
            """.trimIndent()

        val result = session.query(query, emptyMap<String, Any>(), true).toList()
        return result.map {
            (it["entityType"] as Array<String>)
                .filter { !authorizationEntitiesTypes.plus("Entity").contains(it) }
        }.flatten()
    }

    fun getEntities(ids: List<String>?, type: String, rawQuery: String): List<URI> {
        val formattedIds = ids?.map { "'$it'" }
        val pattern = Pattern.compile("([^();|]+)")
        val innerQuery = rawQuery.replace(
            pattern.toRegex()
        ) { matchResult ->
            val parsedQueryTerm = extractComparisonParametersFromQuery(matchResult.value)
            if (parsedQueryTerm.third.isRelationshipTarget()) {
                """
                    EXISTS {
                        MATCH (n)-[:HAS_OBJECT]-()-[:${parsedQueryTerm.first}]->(e)
                        WHERE e.id ${parsedQueryTerm.second} ${parsedQueryTerm.third}
                    }
                """.trimIndent()
            } else {
                val comparableValue = when {
                    parsedQueryTerm.third.isFloat() -> "toFloat('${parsedQueryTerm.third}')"
                    parsedQueryTerm.third.isDateTime() -> "datetime('${parsedQueryTerm.third}')"
                    parsedQueryTerm.third.isDate() -> "date('${parsedQueryTerm.third}')"
                    parsedQueryTerm.third.isTime() -> "localtime('${parsedQueryTerm.third}')"
                    else -> parsedQueryTerm.third
                }
                """
                   EXISTS {
                       MATCH (n)-[:HAS_VALUE]->(p:Property)
                       WHERE p.name = '${parsedQueryTerm.first}'
                       AND p.value ${parsedQueryTerm.second} $comparableValue
                   }
                """.trimIndent()
            }
        }
            .replace(";", " AND ")
            .replace("|", " OR ")

        val matchClause =
            if (type.isEmpty())
                "MATCH (n:Entity)"
            else
                "MATCH (n:`$type`)"

        val idClause =
            if (ids != null)
                """
                    n.id in $formattedIds
                    ${if (innerQuery.isNotEmpty()) " AND " else ""}
                """
            else ""

        val whereClause =
            if (innerQuery.isNotEmpty() || ids != null) " WHERE "
            else ""

        val finalQuery =
            """
            $matchClause
            $whereClause
                $idClause
                $innerQuery
            RETURN n.id as id
            """

        return session.query(finalQuery, emptyMap<String, Any>(), true)
            .map { (it["id"] as String).toUri() }
    }

    fun filterExistingEntitiesAsIds(entitiesIds: List<URI>): List<URI> {
        if (entitiesIds.isEmpty()) {
            return emptyList()
        }

        val query = "MATCH (entity:Entity) WHERE entity.id IN \$entitiesIds RETURN entity.id as id"

        return session.query(query, mapOf("entitiesIds" to entitiesIds.toListOfString()), true)
            .map { (it["id"] as String).toUri() }
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

    private val deleteAttributeQuery =
        """
        OPTIONAL MATCH (attribute)-[:HAS_VALUE]->(propOfAttribute:Property)
        WITH entity, attribute, propOfAttribute
        OPTIONAL MATCH (attribute)-[:HAS_OBJECT]->(relOfAttribute:Relationship)
        DETACH DELETE attribute, propOfAttribute, relOfAttribute
        """.trimIndent()

    private val authorizationEntitiesTypes = listOf(
        AuthorizationService.AUTHORIZATION_ONTOLOGY + "User",
        AuthorizationService.AUTHORIZATION_ONTOLOGY + "Client",
        AuthorizationService.AUTHORIZATION_ONTOLOGY + "Group"
    )

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
