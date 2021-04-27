package com.egm.stellio.entity.model

import com.egm.stellio.shared.util.toUri
import java.net.URI
import java.util.*

data class EntityTypeList(
    val id: URI = "urn:ngsi-ld:EntityTypeList:${UUID.randomUUID()}".toUri(),
    val type: String = "EntityTypeList",
    val typeList: List<String>
)
