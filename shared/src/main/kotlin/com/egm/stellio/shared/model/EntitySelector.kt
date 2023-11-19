package com.egm.stellio.shared.model

import com.fasterxml.jackson.annotation.JsonProperty
import java.net.URI

// an entity type selection as per clause 4.17
typealias EntityTypeSelection = String

/**
 * EntitySelector type as defined in 5.2.33
 */
data class EntitySelector(
    val id: URI?,
    val idPattern: String?,
    @JsonProperty(value = "type")
    val typeSelection: EntityTypeSelection
)
