package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.model.Relationship
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.stereotype.Repository

@Repository
interface RelationshipRepository : Neo4jRepository<Relationship, String>