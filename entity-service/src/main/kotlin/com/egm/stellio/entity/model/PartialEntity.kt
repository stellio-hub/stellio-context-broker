package com.egm.stellio.entity.model

import org.neo4j.ogm.annotation.Id
import org.neo4j.ogm.annotation.NodeEntity
import org.neo4j.ogm.annotation.typeconversion.Convert
import java.net.URI

@NodeEntity
class PartialEntity(

    @Id
    @Convert(UriConverter::class)
    val id: URI
)
