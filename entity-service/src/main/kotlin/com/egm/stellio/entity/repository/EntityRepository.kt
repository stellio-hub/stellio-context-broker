package com.egm.stellio.entity.repository

import com.egm.stellio.entity.model.Entity
import org.springframework.data.neo4j.annotation.Query
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.stereotype.Repository
import java.net.URI

@Repository
interface EntityRepository : Neo4jRepository<Entity, URI> {

    @Query(
        "MATCH (entity:Entity { id: \$id }) " +
            "RETURN entity " +
            "LIMIT 1"
    )
    fun getEntityCoreById(id: String): Entity?

    @Query(
        "MATCH (entity:Entity { id: \$id })-[:HAS_VALUE]->(property:Property)" +
            "OPTIONAL MATCH (property)-[:HAS_VALUE]->(propValue:Property)" +
            "OPTIONAL MATCH (property)-[:HAS_OBJECT]->(relOfProp:Relationship)-[rel]->(relOfPropObject)" +
            "RETURN property, propValue, type(rel) as relType, relOfProp, relOfPropObject.id as relOfPropObjectId"
    )
    fun getEntitySpecificProperties(id: String): List<Map<String, Any>>

    @Query(
        "MATCH (entity:Entity { id: \$id })-[:HAS_VALUE]->(property:Property {id: \$propertyId })" +
            "OPTIONAL MATCH (property)-[:HAS_VALUE]->(propValue:Property)" +
            "OPTIONAL MATCH (property)-[:HAS_OBJECT]->(relOfProp:Relationship)-[rel]->(relOfPropObject)" +
            "RETURN property, propValue, type(rel) as relType, relOfProp, relOfPropObject.id as relOfPropObjectId"
    )
    fun getEntitySpecificProperty(id: String, propertyId: String): List<Map<String, Any>>

    @Query(
        "MATCH (entity:Entity { id: \$id })-[:HAS_OBJECT]->(rel:Relationship)-[r]->(relObject)" +
            "OPTIONAL MATCH (rel)-[:HAS_VALUE]->(propValue:Property)" +
            "OPTIONAL MATCH (rel)-[:HAS_OBJECT]->(relOfRel:Relationship)-[or]->(relOfRelObject)" +
            "RETURN rel, propValue, type(r) as relType, relObject.id as relObjectId, " +
            " relOfRel, type(or) as relOfRelType, relOfRelObject.id as relOfRelObjectId"
    )
    fun getEntityRelationships(id: String): List<Map<String, Any>>
}
