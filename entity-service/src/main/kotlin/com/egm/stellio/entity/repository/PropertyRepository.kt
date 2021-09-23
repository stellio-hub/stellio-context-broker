package com.egm.stellio.entity.repository

import com.egm.stellio.entity.model.Property
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.data.neo4j.repository.query.Query
import org.springframework.stereotype.Repository
import java.net.URI

@Repository
interface PropertyRepository : Neo4jRepository<Property, URI> {

    @Query(
        "MATCH ({ id: \$subjectId })-[:HAS_VALUE]->(p:Property { name: \$propertyName }) " +
            "WHERE p.datasetId IS NULL " +
            "RETURN p"
    )
    fun getPropertyOfSubject(subjectId: URI, propertyName: String): Property

    @Query(
        "MATCH ({ id: \$subjectId })-[:HAS_VALUE]->(p:Property { name: \$propertyName, datasetId: \$datasetId })" +
            "RETURN p"
    )
    fun getPropertyOfSubject(subjectId: URI, propertyName: String, datasetId: URI): Property
}
