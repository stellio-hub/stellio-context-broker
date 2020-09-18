package com.egm.stellio.entity.util

import com.egm.stellio.entity.model.Property
import org.springframework.data.neo4j.annotation.Query
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.stereotype.Repository

@Repository
interface PropertyRepositoryTest : Neo4jRepository<Property, String> {
    @Query(
        "MATCH (property:Property { id: \$id }) " +
            "RETURN property " +
            "LIMIT 1"
    )
    fun getPropertyCoreById(id: String): Property?

    @Query(
        "MATCH (property:Property { id: \$id })-[:HAS_VALUE]->(propOfProp:Property)" +
            "RETURN propOfProp"
    )
    fun getPropertySpecificProperties(id: String): List<Map<String, Any>>

    @Query(
        "MATCH (property:Property { id: \$id })-[:HAS_OBJECT]->(rel:Relationship)-[r]->(relObject:Entity)" +
            "RETURN rel, type(r) as relType, relObject"
    )
    fun getPropertyRelationships(id: String): List<Map<String, Any>>

    @Query(
        "MATCH (property:Property { id: \$id }) " +
            "DELETE property "
    )
    override fun deleteById(id: String)
}
