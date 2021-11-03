package com.egm.stellio.entity.repository

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.model.toRelationshipTypeName
import com.egm.stellio.shared.model.NgsiLdGeoPropertyInstance
import com.egm.stellio.shared.model.NgsiLdGeoPropertyInstance.Companion.toWktFormat
import com.egm.stellio.shared.model.NgsiLdPropertyInstance
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.toListOfString
import com.egm.stellio.shared.util.toUri
import org.springframework.data.neo4j.core.Neo4jClient
import org.springframework.data.neo4j.core.mappedBy
import org.springframework.stereotype.Component
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset

sealed class SubjectNodeInfo(val id: URI, val label: String)
class EntitySubjectNode(id: URI) : SubjectNodeInfo(id, "Entity")
class AttributeSubjectNode(id: URI) : SubjectNodeInfo(id, "Attribute")

@Component
class Neo4jRepository(
    private val neo4jClient: Neo4jClient
) {

    fun mergePartialWithNormalEntity(id: URI): Int {
        val query =
            """
            MATCH (pe:PartialEntity { id: ${'$'}id })
            WITH pe
            MATCH (e:Entity { id: ${'$'}id }) 
            WITH head(collect([e,pe])) as nodes
            CALL apoc.refactor.mergeNodes(nodes, { properties:"discard" })
            YIELD node
            REMOVE node:PartialEntity
            RETURN node
        """

        return neo4jClient.query(query).bind(id.toString()).to("id").run().counters().nodesDeleted()
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
            "subjectId" to subjectNodeInfo.id.toString()
        )
        return neo4jClient.query(query).bindAll(parameters).run().counters().containsUpdates()
    }

    fun createRelationshipOfSubject(
        subjectNodeInfo: SubjectNodeInfo,
        relationship: Relationship,
        targetId: URI
    ): Boolean {
        val relationshipType = relationship.type[0].toRelationshipTypeName()

        // first search for an existing target entity or partial entity with this id
        val queryForTargetExistence =
            """
            MATCH (target:Entity)
            WHERE target.id = ${'$'}targetId
            RETURN labels(target) as labels
            UNION MATCH (target:PartialEntity)
            WHERE target.id = ${'$'}targetId
            RETURN labels(target) as labels
            """.trimIndent()
        val parametersForExistence = mapOf(
            "targetId" to targetId.toString()
        )

        val resultForExistence =
            neo4jClient.query(queryForTargetExistence).bindAll(parametersForExistence).fetch().first()
        val targetAlreadyExists = resultForExistence.isPresent
        // if the target exists, find whether it is an entity or a partial entity
        val labelForExisting =
            if (targetAlreadyExists)
                resultForExistence
                    .get()
                    .values
                    .map { it as List<String> }
                    .flatten()
                    .first { it == "Entity" || it == "PartialEntity" }
            else ""

        val query =
            if (targetAlreadyExists)
                """
                MATCH (subject:${subjectNodeInfo.label} { id: ${'$'}subjectId })
                WITH subject
                MATCH (target:$labelForExisting { id: ${'$'}targetId })
                CREATE (subject)-[:HAS_OBJECT]->(r:Attribute:Relationship:`${relationship.type[0]}` ${'$'}props)
                        -[:$relationshipType]->(target)
                """
            else
                """
                MATCH (subject:${subjectNodeInfo.label} { id: ${'$'}subjectId })
                MERGE (target:PartialEntity { id: ${'$'}targetId })
                CREATE (subject)-[:HAS_OBJECT]->(r:Attribute:Relationship:`${relationship.type[0]}` ${'$'}props)
                        -[:$relationshipType]->(target)
                """.trimIndent()

        val parameters = mapOf(
            "props" to relationship.nodeProperties(),
            "subjectId" to subjectNodeInfo.id.toString(),
            "targetId" to targetId.toString()
        )

        return neo4jClient.query(query).bindAll(parameters).run().counters().containsUpdates()
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

        return neo4jClient.query(query)
            .bind(subjectId.toString()).to("subjectId")
            .run().counters().propertiesSet()
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
            WHERE attribute.datasetId IS NULL
            """
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

        return neo4jClient.query(matchQuery + deleteAttributeQuery + createAttributeQuery)
            .bindAll(parameters)
            .run().counters().nodesDeleted()
    }

    fun updateEntityModifiedDate(entityId: URI): Int {
        val query =
            """
            MERGE (entity:Entity { id: ${'$'}entityId })
            ON MATCH SET entity.modifiedAt = datetime("${Instant.now().atZone(ZoneOffset.UTC)}")
        """

        return neo4jClient.query(query).bind(entityId.toString()).to("entityId").run().counters().propertiesSet()
    }

    fun hasRelationshipOfType(subjectNodeInfo: SubjectNodeInfo, relationshipType: String): Boolean {
        val query =
            """
            MATCH (a:${subjectNodeInfo.label} { id: ${'$'}attributeId })-[:HAS_OBJECT]->(rel)-[:$relationshipType]->()
            RETURN a.id
            """.trimIndent()

        return neo4jClient.query(query)
            .bind(subjectNodeInfo.id.toString()).to("attributeId")
            .fetch().first().isPresent
    }

    fun hasPropertyOfName(subjectNodeInfo: SubjectNodeInfo, propertyName: String): Boolean {
        val query =
            """
            MATCH (a:${subjectNodeInfo.label} { id: ${'$'}attributeId })-[:HAS_VALUE]->(property:Property { name: ${'$'}propertyName })
            RETURN a.id
            """.trimIndent()

        val parameters = mapOf(
            "attributeId" to subjectNodeInfo.id.toString(),
            "propertyName" to propertyName
        )
        return neo4jClient.query(query).bindAll(parameters).fetch().first().isPresent
    }

    fun hasGeoPropertyOfName(subjectNodeInfo: SubjectNodeInfo, geoPropertyName: String): Boolean {
        val query =
            """
            MATCH (a:${subjectNodeInfo.label} { id: ${'$'}attributeId }) WHERE a.$geoPropertyName IS NOT NULL
            RETURN a.id
            """.trimIndent()

        return neo4jClient.query(query)
            .bind(subjectNodeInfo.id.toString()).to("attributeId")
            .fetch().first().isPresent
    }

    fun hasPropertyInstance(subjectNodeInfo: SubjectNodeInfo, propertyName: String, datasetId: URI? = null): Boolean {
        val query = if (datasetId == null)
            """
            MATCH (a:${subjectNodeInfo.label} { id: ${'$'}attributeId })-[:HAS_VALUE]->(property:Property { name: ${'$'}propertyName })
            WHERE property.datasetId IS NULL
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

        return neo4jClient.query(query).bindAll(parameters).fetch().first().isPresent
    }

    fun hasRelationshipInstance(
        subjectNodeInfo: SubjectNodeInfo,
        relationshipType: String,
        datasetId: URI? = null
    ): Boolean {
        val query = if (datasetId == null)
            """
            MATCH (a:${subjectNodeInfo.label} { id: ${'$'}attributeId })-[:HAS_OBJECT]->(rel:Relationship)-[:$relationshipType]->()
            WHERE rel.datasetId IS NULL
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
        return neo4jClient.query(query).bindAll(parameters).fetch().first().isPresent
    }

    fun updateRelationshipTargetOfSubject(
        subjectId: URI,
        relationshipType: String,
        newRelationshipObjectId: URI,
        datasetId: URI? = null
    ): Boolean {
        val matchQuery = if (datasetId == null)
            """
            MATCH ({ id: ${'$'}subjectId })-[:HAS_OBJECT]->(r:Relationship)-[v:$relationshipType]->()
            """.trimIndent()
        else
            """
            MATCH ({ id: ${'$'}subjectId })-[:HAS_OBJECT]->(r:Relationship { datasetId: ${'$'}datasetId })
            -[v:$relationshipType]->()
            """.trimIndent()

        val relationshipTypeQuery = matchQuery + """
            MERGE (target { id: ${'$'}newRelationshipObjectId })
            ON CREATE SET target:PartialEntity
            DETACH DELETE v
            MERGE (r)-[:$relationshipType]->(target)
        """.trimIndent()

        val parameters = mapOf(
            "subjectId" to subjectId.toString(),
            "newRelationshipObjectId" to newRelationshipObjectId.toString(),
            "datasetId" to datasetId?.toString()
        )

        return neo4jClient.query(relationshipTypeQuery).bindAll(parameters).run().counters().containsUpdates()
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
            SET a.objectId = ${'$'}newRelationshipObjectId
            """.trimIndent()

        val parameters = mapOf(
            "attributeId" to attributeId.toString(),
            "oldRelationshipObjectId" to oldRelationshipObjectId.toString(),
            "newRelationshipObjectId" to newRelationshipObjectId.toString()
        )
        return neo4jClient.query(relationshipTypeQuery).bindAll(parameters).run().counters().nodesDeleted()
    }

    fun updateLocationPropertyOfEntity(entityId: URI, geoProperty: NgsiLdGeoPropertyInstance): Int {
        val query =
            """
            MERGE (entity:Entity { id: "$entityId" })
            ON MATCH SET entity.location = "${toWktFormat(geoProperty.geoPropertyType, geoProperty.coordinates)}"
            """
        return neo4jClient.query(query).run().counters().propertiesSet()
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

        val queryStatistics = neo4jClient.query(query).bind(entityId.toString()).to("entityId").run().counters()
        return Pair(queryStatistics.nodesDeleted(), queryStatistics.relationshipsDeleted())
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

        val queryStatistics = neo4jClient.query(query).bind(entityId.toString()).to("entityId").run().counters()
        return Pair(queryStatistics.nodesDeleted(), queryStatistics.relationshipsDeleted())
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
            WHERE attribute.datasetId IS NULL
            """
        else
            """
            MATCH (entity:${subjectNodeInfo.label} { id: ${'$'}entityId })-[:HAS_VALUE]->(attribute:Property { name: ${'$'}propertyName, datasetId: ${'$'}datasetId})
            """.trimIndent()

        val parameters = mapOf(
            "entityId" to subjectNodeInfo.id.toString(),
            "propertyName" to propertyName,
            "datasetId" to datasetId?.toString()
        )

        return neo4jClient.query(matchQuery + deleteAttributeQuery)
            .bindAll(parameters)
            .run().counters().nodesDeleted()
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
            WHERE attribute.datasetId IS NULL 
            """.trimIndent()
        else
            """
            MATCH (entity:${subjectNodeInfo.label} { id: ${'$'}entityId })-[:HAS_OBJECT]->(attribute:Relationship { datasetId: ${'$'}datasetId})-[:$relationshipType]->()
            """.trimIndent()

        val parameters = mapOf(
            "entityId" to subjectNodeInfo.id.toString(),
            "datasetId" to datasetId?.toString()
        )

        return neo4jClient.query(matchQuery + deleteAttributeQuery).bindAll(parameters).run().counters().nodesDeleted()
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

        val result = neo4jClient.query(query).fetch().all()
        if (result.isEmpty())
            return emptyMap()

        val entityCount = (result.first()["entityCount"] as Long).toInt()
        val entityWithLocationCount = (result.first()["entityWithLocationCount"] as Long).toInt()
        return mapOf(
            "properties" to (result.first()["propertyNames"] as List<String>).toSet(),
            "relationships" to (result.first()["relationshipNames"] as List<String>)
                .filter { it !in listOf("Attribute", "Relationship") }.toSet(),
            "geoProperties" to
                if (entityWithLocationCount > 0) setOf(NGSILD_LOCATION_PROPERTY) else emptySet(),
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

        val result = neo4jClient.query(query).fetch().all()
        return result.map { rowResult ->
            val entityWithLocationCount = (rowResult["entityWithLocationCount"] as Long).toInt()
            val entityTypes = (rowResult["entityType"] as List<String>)
                .filter { !authorizationEntitiesTypes.plus("Entity").contains(it) }
            entityTypes.map { entityType ->
                mapOf(
                    "entityType" to entityType,
                    "properties" to (rowResult["propertyNames"] as List<Any>).toSet(),
                    "relationships" to (rowResult["relationshipNames"] as List<Any>)
                        .filter { it !in listOf("Attribute", "Relationship") }.toSet(),
                    "geoProperties" to
                        if (entityWithLocationCount > 0) setOf(NGSILD_LOCATION_PROPERTY) else emptySet()
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

        val result = neo4jClient.query(query).fetch().all()
        return result.map {
            (it["entityType"] as List<String>)
                .filter { !authorizationEntitiesTypes.plus("Entity").contains(it) }
        }.flatten()
    }

    fun getAttribute(): List<String> {
        val query =
            """
                MATCH (a:Attribute)
                OPTIONAL MATCH (a:Attribute)<-[:HAS_OBJECT|:HAS_VALUE]-(entity:Entity)
                RETURN DISTINCT(labels(a))+collect(DISTINCT(a.name)) as attribute,
                count(entity.location) as entityWithLocationCount
            """.trimIndent()

        val result = neo4jClient.query(query).fetch().all()
        return result.map { rowResult ->
            val entityWithLocationCount = (rowResult["entityWithLocationCount"] as Long).toInt()
            var attributeList = (rowResult["attribute"] as List<String>)
                .filter { it !in listOf("Attribute", "Relationship", "Property") }.toList()
            if (entityWithLocationCount > 0) { attributeList += NGSILD_LOCATION_PROPERTY }
            attributeList
        }.flatten()
    }

    fun getAttributeDetails(): List<Map<String, Any>> {
        val query =
            """
                OPTIONAL MATCH (prop:Attribute)<-[:HAS_VALUE]-(entity:Entity)
                OPTIONAL MATCH (rel:Attribute)<-[:HAS_OBJECT]-(entity:Entity)
                RETURN DISTINCT (labels(rel)) as relation, 
                labels(entity) as typeNames,
                collect(prop.name) as property,
                count(entity.location) as entityWithLocationCount
            """.trimIndent()

        val result = neo4jClient.query(query).fetch().all()
        return result.map { rowResult ->
            val property = (rowResult["property"] as List<String>)
            val relation = (rowResult["relation"] as List<String>)
            var attribute: List<String> = property.plus(relation)
                .filter { it !in listOf("Attribute", "Relationship", "Property") }.toList()
            val entityWithLocationCount = (rowResult["entityWithLocationCount"] as Long).toInt()
            if (entityWithLocationCount > 0) {
                val location: List<String> = listOf(NGSILD_LOCATION_PROPERTY)
                attribute = property.plus(location).plus(relation)
                    .filter { it !in listOf("Attribute", "Relationship", "Property") }.toList()
            }
            attribute.map { attribute ->
                mapOf(
                    "attribute" to attribute,
                    "typeNames" to (rowResult["typeNames"] as List<Any>)
                        .filter { !authorizationEntitiesTypes.plus("Entity").contains(it) }.toSet(),
                )
            }
        }.flatten()
    }
    fun getAttributeInformation(expandedType: String): Map<String, Any> {
        val relationshipQuery =
            """
                MATCH (a:Attribute:`$expandedType`)
                OPTIONAL MATCH (a)<-[:HAS_OBJECT|:HAS_VALUE]-(entity:Entity)
                return DISTINCT (labels(a)) as attributeTypes,
                labels(entity) as typeNames,
                count(a) as attributeCount,
                count(entity.location) as entityLocationCount
            """.trimIndent()
        val propertyQuery =
            """
                MATCH (a:Attribute{name:"$expandedType"})
                OPTIONAL MATCH (a)<-[:HAS_VALUE]-(entity:Entity)
                return DISTINCT (labels(a)) as attributeTypes,
                labels(entity) as typeNames,
                count(a) as attributeCount,
                count(entity.location) as entityLocationCount
            """.trimIndent()

        var result = neo4jClient.query(relationshipQuery).fetch().all()
        if (result.isEmpty())
            result = neo4jClient.query(propertyQuery).fetch().all()
        if (result.isEmpty())
            return emptyMap()
        val attributeCount = (result.first()["attributeCount"] as Long).toInt()
        var attributeType = (result.first()["attributeTypes"] as List<String>)
            .filter { it !in listOf("Attribute", expandedType) }.toList()
        val entityWithLocationCount = (result.first()["entityLocationCount"] as Long).toInt()
        if (entityWithLocationCount > 0) { attributeType += "GeoProperty" }
        return mapOf(
            "attributeName" to expandedType,
            "attributeTypes" to attributeType,
            "typeNames" to (result.first()["typeNames"] as List<String>)
                .filter { !authorizationEntitiesTypes.plus("Entity").contains(it) }.toSet(),
            "attributeCount" to attributeCount
        )
    }

    fun filterExistingEntitiesAsIds(entitiesIds: List<URI>): List<URI> {
        if (entitiesIds.isEmpty()) {
            return emptyList()
        }

        val query = "MATCH (entity:Entity) WHERE entity.id IN \$entitiesIds RETURN entity.id as id"

        return neo4jClient.query(query).bind(entitiesIds.toListOfString()).to("entitiesIds")
            .mappedBy { _, record -> (record["id"].asString()).toUri() }
            .all()
            .toList()
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
}
