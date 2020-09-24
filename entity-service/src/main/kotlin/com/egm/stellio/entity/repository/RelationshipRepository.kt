package com.egm.stellio.entity.repository

import com.egm.stellio.entity.model.Relationship
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.stereotype.Repository
import java.net.URI

@Repository
interface RelationshipRepository : Neo4jRepository<Relationship, URI>
