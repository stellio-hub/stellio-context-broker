package com.egm.stellio.entity.authorization

import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_IS_MEMBER_OF
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NAME_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.buildJsonLdExpandedProperty
import java.net.URI

data class Group(
    val id: URI,
    val type: String = GROUP_TYPE,
    val isMember: Boolean? = false,
    val name: String
) {
    fun serializeProperties(): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        resultEntity[JsonLdUtils.JSONLD_ID] = id.toString()
        resultEntity[JsonLdUtils.JSONLD_TYPE] = type

        resultEntity[NGSILD_NAME_PROPERTY] = buildJsonLdExpandedProperty(name)

        isMember?.run {
            resultEntity[AUTH_REL_IS_MEMBER_OF] = buildJsonLdExpandedProperty(isMember.toString())
        }
        return resultEntity
    }
}
