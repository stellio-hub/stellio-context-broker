package com.egm.stellio.search.discovery.model

import com.egm.stellio.shared.util.UriUtils.toUri
import java.net.URI
import java.util.UUID

data class AttributeList(
    val id: URI = "urn:ngsi-ld:AttributeList:${UUID.randomUUID()}".toUri(),
    val type: String = "AttributeList",
    val attributeList: List<String>
)
