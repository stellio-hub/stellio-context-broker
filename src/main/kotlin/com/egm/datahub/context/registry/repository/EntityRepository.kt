package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.model.Entity
import org.springframework.data.neo4j.annotation.Query
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.stereotype.Repository

@Repository
interface EntityRepository : Neo4jRepository<Entity, String> {

    @Query("""
        MATCH (entity:Entity { id: {id} })
        RETURN entity
    """)
    fun getEntityCoreById(id: String): List<Map<String, Any>>

    @Query("""
        MATCH (entity:Entity { id: {id} })-[:HAS_VALUE]->(property)
        OPTIONAL MATCH (property)-[rel]->(relOfProp:Entity)
        WHERE type(rel) <> 'HAS_OBJECT'
        RETURN property, type(rel) as relType, relOfProp
    """)
    fun getEntitySpecificProperties(id: String): List<Map<String, Any>>

    @Query("""
        MATCH (entity:Entity { id: {id} })-[:HAS_OBJECT]->(relEntity)-[rel]->(relTarget)
        WHERE type(rel) <> 'HAS_OBJECT'
        RETURN relEntity, type(rel) as relType, relTarget
    """)
    fun getEntityRelationships(id: String): List<Map<String, Any>>
}
