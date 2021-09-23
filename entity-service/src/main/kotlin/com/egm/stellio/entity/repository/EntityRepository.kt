package com.egm.stellio.entity.repository

import com.egm.stellio.entity.model.Entity
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.data.neo4j.repository.query.Query
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
        "MATCH ({ id: \$subjectId })-[:HAS_OBJECT]->(r:Relationship { datasetId: \$datasetId }) " +
            "-[:`:#{literal(#relationshipType)}`]->(e:Entity)" +
            " RETURN e"
    )
    fun getRelationshipTargetOfSubject(subjectId: URI, relationshipType: String, datasetId: URI): Entity?

    @Query(
        "MATCH ({ id: \$subjectId })-[:HAS_OBJECT]->(r:Relationship) " +
            "-[:`:#{literal(#relationshipType)}`]->(e:Entity)" +
            " RETURN e"
    )
    fun getRelationshipTargetOfSubject(subjectId: URI, relationshipType: String): Entity?
}
