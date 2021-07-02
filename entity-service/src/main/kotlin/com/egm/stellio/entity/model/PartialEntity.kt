package com.egm.stellio.entity.model

import com.egm.stellio.entity.config.Neo4jUriPropertyConverter
import org.springframework.data.neo4j.core.convert.ConvertWith
import org.springframework.data.neo4j.core.schema.Id
import org.springframework.data.neo4j.core.schema.Node
import java.net.URI

@Node
data class PartialEntity(

    @Id
    @ConvertWith(converter = Neo4jUriPropertyConverter::class)
    val id: URI
)
