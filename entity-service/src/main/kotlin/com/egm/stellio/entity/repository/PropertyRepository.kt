package com.egm.stellio.entity.repository

import com.egm.stellio.entity.model.Property
import org.springframework.data.neo4j.repository.Neo4jRepository
import org.springframework.stereotype.Repository
import java.net.URI

@Repository
interface PropertyRepository : Neo4jRepository<Property, URI>
