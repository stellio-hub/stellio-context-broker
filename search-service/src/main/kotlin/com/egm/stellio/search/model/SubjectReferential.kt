package com.egm.stellio.search.model

import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.SubjectType
import org.springframework.data.annotation.Id

data class SubjectReferential(
    @Id
    val subjectId: Sub,
    val subjectType: SubjectType,
    val serviceAccountId: Sub? = null,
    val globalRoles: List<GlobalRole>? = null,
    val groupsMemberships: List<Sub>? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SubjectReferential

        if (subjectId != other.subjectId) return false

        return true
    }

    override fun hashCode(): Int = subjectId.hashCode()
}
