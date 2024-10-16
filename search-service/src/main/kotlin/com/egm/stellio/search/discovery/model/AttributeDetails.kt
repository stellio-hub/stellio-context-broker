package com.egm.stellio.search.discovery.model

import java.net.URI

data class AttributeDetails(
    val id: URI,
    val type: String = "Attribute",
    val attributeName: String,
    val typeNames: Set<String>
)
