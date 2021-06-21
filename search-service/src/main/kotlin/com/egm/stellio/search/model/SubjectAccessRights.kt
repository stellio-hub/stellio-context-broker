package com.egm.stellio.search.model

import java.net.URI

data class SubjectAccessRights(
    val subjectId: URI,
    val subjectType: SubjectType,
    val globalRole: String? = null,
    val allowedReadEntities: Array<String>? = null,
    val allowedWriteEntities: Array<String>? = null
) {
    enum class SubjectType {
        USER,
        GROUP,
        CLIENT
    }

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

fun getSubjectTypeFromSubjectId(subjectId: URI): SubjectAccessRights.SubjectType {
    val type = subjectId.toString().split(":")[2]
    return SubjectAccessRights.SubjectType.valueOf(type.toUpperCase())
}
