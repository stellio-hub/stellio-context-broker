package com.egm.stellio.search.discovery.model

import com.egm.stellio.shared.util.UriUtils.toUri
import java.net.URI
import java.util.*

data class EntityTypeList(
    val id: URI = "urn:ngsi-ld:EntityTypeList:${UUID.randomUUID()}".toUri(),
    val type: String = "EntityTypeList",
    val typeList: List<String>
)
