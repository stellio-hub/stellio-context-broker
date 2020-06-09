package com.egm.stellio.entity.repository

import com.egm.stellio.entity.model.Entity
import org.springframework.data.neo4j.annotation.Query
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.stereotype.Repository

@Repository
interface EntityRepository : Neo4jRepository<Entity, String> {

    @Query("MATCH (entity:Entity { id: \$id })" +
            "RETURN entity"
    )
    fun getEntityCoreById(id: String): List<Map<String, Any>>

    @Query("MATCH (entity:Entity { id: \$id })-[:HAS_VALUE]->(property:Property)" +
            "OPTIONAL MATCH (property)-[:HAS_VALUE]->(propValue:Property)" +
            "OPTIONAL MATCH (property)-[:HAS_OBJECT]->(relOfProp:Relationship)-[rel]->(relOfPropObject:Entity)" +
            "WHERE type(rel) <> 'HAS_OBJECT'" +
            "RETURN property, propValue, type(rel) as relType, relOfProp, relOfPropObject"
    )
    fun getEntitySpecificProperties(id: String): List<Map<String, Any>>

    @Query("MATCH (entity:Entity { id: \$id })-[:HAS_VALUE]->(property:Property {id: \$propertyId })" +
            "OPTIONAL MATCH (property)-[:HAS_VALUE]->(propValue:Property)" +
            "OPTIONAL MATCH (property)-[:HAS_OBJECT]->(relOfProp:Relationship)-[rel]->(relOfPropObject:Entity)" +
            "WHERE type(rel) <> 'HAS_OBJECT'" +
            "RETURN property, propValue, type(rel) as relType, relOfProp, relOfPropObject"
    )
    fun getEntitySpecificProperty(id: String, propertyId: String): List<Map<String, Any>>

    @Query("MATCH (entity:Entity { id: \$id })-[:HAS_OBJECT]->(rel:Relationship)-[r]->(relObject:Entity)" +
            "WHERE type(r) <> 'HAS_OBJECT'" +
            "OPTIONAL MATCH (rel)-[:HAS_OBJECT]->(relOfRel:Relationship)-[or]->(relOfRelObject:Entity)" +
            "WHERE type(or) <> 'HAS_OBJECT'" +
            "RETURN rel, type(r) as relType, relObject, relOfRel, type(or) as relOfRelType, relOfRelObject"
    )
    fun getEntityRelationships(id: String): List<Map<String, Any>>
}
