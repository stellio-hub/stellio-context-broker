package com.egm.stellio.search.model

import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.SubjectType
import org.springframework.data.annotation.Id
import java.util.UUID

data class SubjectReferential(
    @Id
    val subjectId: UUID,
    val subjectType: SubjectType,
    val globalRoles: List<GlobalRole>? = null,
    val groupsMemberships: List<UUID>? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SubjectReferential

        if (subjectId != other.subjectId) return false

        return true
    }

    override fun hashCode(): Int {
        return subjectId.hashCode()
    }
}
