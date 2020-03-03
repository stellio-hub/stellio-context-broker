package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.model.Attribute
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.stereotype.Repository

@Repository
interface AttributeRepository : Neo4jRepository<Attribute, String>