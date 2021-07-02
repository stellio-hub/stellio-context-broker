package com.egm.stellio.entity.repository

import com.egm.stellio.entity.model.Relationship
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.data.neo4j.repository.query.Query
import org.springframework.stereotype.Repository
import java.net.URI

@Repository
interface RelationshipRepository : Neo4jRepository<Relationship, URI> {

    @Query(
        "MATCH ({ id: \$subjectId })-[:HAS_OBJECT]->(r:Relationship)-[:`:#{literal(#relationshipType)}`]->() " +
            "WHERE r.datasetId IS NULL " +
            "RETURN r"
    )
    fun getRelationshipOfSubject(subjectId: URI, relationshipType: String): Relationship

    @Query(
        "MATCH ({ id: \$subjectId })-[:HAS_OBJECT]->(r:Relationship { datasetId: \$datasetId })" +
            "   -[:`:#{literal(#relationshipType)}`]->()" +
            "RETURN r"
    )
    fun getRelationshipOfSubject(subjectId: URI, relationshipType: String, datasetId: URI): Relationship
}
