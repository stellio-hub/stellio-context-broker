package com.egm.stellio.search.authorization.model

import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_NAME
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_IS_MEMBER_OF
import com.egm.stellio.shared.util.AuthContextModel.GROUP_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyValue

data class Group(
    val id: String,
    val type: String = GROUP_TYPE,
    val isMember: Boolean = false,
    val name: String
) {
    fun serializeProperties(): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        resultEntity[JsonLdUtils.JSONLD_ID] = GROUP_ENTITY_PREFIX + id
        resultEntity[JsonLdUtils.JSONLD_TYPE] = listOf(type)

        resultEntity[AUTH_PROP_NAME] = buildExpandedPropertyValue(name)

        isMember.run {
            resultEntity[AUTH_REL_IS_MEMBER_OF] = buildExpandedPropertyValue(isMember.toString())
        }
        return resultEntity
    }
}
