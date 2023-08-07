package com.egm.stellio.search.authorization

import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_FAMILY_NAME
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_GIVEN_NAME
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_USERNAME
import com.egm.stellio.shared.util.AuthContextModel.USER_ENTITY_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedProperty

data class User(
    val id: String,
    val type: String = USER_TYPE,
    val username: String,
    val givenName: String? = null,
    val familyName: String? = null
) {
    fun serializeProperties(): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        resultEntity[JSONLD_ID] = USER_ENTITY_PREFIX + id
        resultEntity[JSONLD_TYPE] = listOf(type)

        resultEntity[AUTH_PROP_USERNAME] = buildExpandedProperty(username)

        givenName.run {
            resultEntity[AUTH_PROP_GIVEN_NAME] = buildExpandedProperty(givenName.toString())
        }

        familyName.run {
            resultEntity[AUTH_PROP_FAMILY_NAME] = buildExpandedProperty(familyName.toString())
        }

        return resultEntity
    }
}
