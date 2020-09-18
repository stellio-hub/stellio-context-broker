package com.egm.stellio.entity.util

import com.egm.stellio.entity.model.Relationship
import org.springframework.data.neo4j.annotation.Query
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.stereotype.Repository

@Repository
interface RelationshipRepositoryTest : Neo4jRepository<Relationship, String> {
    @Query(
        "MATCH (relationship:Relationship { id: \$id }) " +
            "RETURN relationship " +
            "LIMIT 1"
    )
    fun getRelationshipCoreById(id: String): Relationship?

    @Query(
        "MATCH (relationship:Relationship { id: \$id }) " +
            "DELETE relationship "
    )
    override fun deleteById(id: String)
}
