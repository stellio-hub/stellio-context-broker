package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.ExpandedTerm
import java.net.URI

data class EntityInfo(
    val id: URI?,
    val idPattern: String?,
    val type: ExpandedTerm
)
