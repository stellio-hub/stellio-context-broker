package com.egm.stellio.subscription.model

import java.net.URI

data class EntityTypeSelector(
    val id: URI?,
    val idPattern: String?,
    var type: String
)
