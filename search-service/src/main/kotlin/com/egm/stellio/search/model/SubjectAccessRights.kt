package com.egm.stellio.search.model

import com.egm.stellio.shared.util.SubjectType
import org.springframework.data.annotation.Id
import java.util.UUID

data class SubjectAccessRights(
    @Id
    val subjectId: UUID,
    val subjectType: SubjectType,
    val globalRole: String? = null,
    val allowedReadEntities: Array<String>? = null,
    val allowedWriteEntities: Array<String>? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SubjectAccessRights

        if (subjectId != other.subjectId) return false

        return true
    }

    override fun hashCode(): Int {
        return subjectId.hashCode()
    }
}
