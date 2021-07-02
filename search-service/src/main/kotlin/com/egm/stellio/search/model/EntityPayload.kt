package com.egm.stellio.search.model

import java.net.URI

data class EntityPayload(
    val entityId: URI,
    val entityPayload: String
)
