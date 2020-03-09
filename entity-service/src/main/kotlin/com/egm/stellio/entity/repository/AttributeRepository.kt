package com.egm.stellio.entity.repository

import com.egm.stellio.entity.model.Attribute
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.stereotype.Repository

@Repository
interface AttributeRepository : Neo4jRepository<Attribute, String>