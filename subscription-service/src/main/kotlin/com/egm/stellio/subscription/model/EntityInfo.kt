package com.egm.stellio.subscription.model

import com.egm.stellio.shared.model.ExpandedTerm
import java.net.URI

data class EntityInfo(
    val id: URI?,
    val idPattern: String?,
    var type: ExpandedTerm
)
