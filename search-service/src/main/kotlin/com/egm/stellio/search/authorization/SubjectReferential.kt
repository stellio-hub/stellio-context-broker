package com.egm.stellio.search.authorization

import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.SubjectType
import org.springframework.data.annotation.Id

data class SubjectReferential(
    @Id
    val subjectId: Sub,
    val subjectType: SubjectType,
    val subjectInfo: String,
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

    fun getSubjectInfoValue(): Map<String, String> =
        subjectInfo.deserializeAsMap()[JSONLD_VALUE] as Map<String, String>
}

internal fun Map<String, Any>.toSubjectInfo(): String {
    val keyValueRepresentaion = this.mapValues {
        (it.value as Map<String, Any>)[JSONLD_VALUE] as String
    }
    return JsonUtils.serializeObject(
        mapOf(
            "type" to "Property",
            "value" to keyValueRepresentaion
        )
    )
}
