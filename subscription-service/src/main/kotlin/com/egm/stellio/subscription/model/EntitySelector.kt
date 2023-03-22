package com.egm.stellio.subscription.model

import java.net.URI

/**
 * EntitySelector type as defined in 5.2.33
 */
data class EntitySelector(
    val id: URI?,
    val idPattern: String?,
    // type is a type selection as per clause 4.17
    val type: String
)
