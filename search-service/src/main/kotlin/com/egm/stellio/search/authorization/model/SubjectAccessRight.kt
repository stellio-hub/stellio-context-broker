package com.egm.stellio.search.authorization.model

import com.egm.stellio.shared.util.AccessRight
import java.net.URI
import java.util.UUID

data class SubjectAccessRight(
    val subjectId: UUID,
    val accessRight: AccessRight,
    val entityId: URI
)
