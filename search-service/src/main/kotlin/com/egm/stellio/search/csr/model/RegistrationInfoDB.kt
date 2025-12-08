package com.egm.stellio.search.csr.model

import com.fasterxml.jackson.annotation.JsonProperty
import java.net.URI

// used for instantiation in database
data class RegistrationInfoDBWriter(
    val entities: List<EntityInfoDB>? = null,
    val propertyNames: List<String>? = null,
    val relationshipNames: List<String>? = null
) {
    constructor(registrationInfo: RegistrationInfo) : this(
        registrationInfo.entities?.map { EntityInfoDB(it) },
        registrationInfo.propertyNames,
        registrationInfo.relationshipNames
    )
}

data class EntityInfoDB(
    val id: URI? = null,
    val idPattern: String? = null,
    // types should always be a list in database
    @JsonProperty("type")
    val types: List<String>
) {
    constructor(info: EntityInfo) : this(
        info.id,
        info.idPattern,
        info.types
    )
}
