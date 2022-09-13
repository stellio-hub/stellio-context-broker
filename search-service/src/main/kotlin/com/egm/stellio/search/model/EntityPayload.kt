package com.egm.stellio.search.model

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import java.net.URI
import java.time.ZonedDateTime

data class EntityPayload(
    val entityId: URI,
    val types: List<ExpandedTerm>,
    val createdAt: ZonedDateTime,
    val modifiedAt: ZonedDateTime? = null,
    // creation time contexts
    // FIXME only stored because needed to compact types at deletion time...
    val contexts: List<String>,
    val entityPayload: String? = null,
    val specificAccessPolicy: SpecificAccessPolicy? = null
)
