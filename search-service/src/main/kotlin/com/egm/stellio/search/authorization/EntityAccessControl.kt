package com.egm.stellio.search.authorization

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_RIGHT
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.buildJsonLdExpandedDateTime
import com.egm.stellio.shared.util.JsonLdUtils.buildJsonLdExpandedProperty
import com.egm.stellio.shared.util.JsonLdUtils.buildJsonLdExpandedRelationship
import java.net.URI
import java.time.ZonedDateTime

data class EntityAccessControl(
    val id: URI,
    val types: List<ExpandedTerm>,
    val right: AccessRight,
    val createdAt: ZonedDateTime,
    val modifiedAt: ZonedDateTime? = null,
    val specificAccessPolicy: AuthContextModel.SpecificAccessPolicy? = null,
    val rCanAdminUsers: List<URI>? = null,
    val rCanWriteUsers: List<URI>? = null,
    val rCanReadUsers: List<URI>? = null
) {
    fun serializeProperties(includeSysAttrs: Boolean): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()

        resultEntity[JsonLdUtils.JSONLD_ID] = id.toString()
        resultEntity[JsonLdUtils.JSONLD_TYPE] = types
        resultEntity[AUTH_PROP_RIGHT] = buildJsonLdExpandedProperty(right.attributeName)

        if (includeSysAttrs) {
            resultEntity[JsonLdUtils.NGSILD_CREATED_AT_PROPERTY] = buildJsonLdExpandedDateTime(createdAt)
            modifiedAt?.run {
                resultEntity[JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY] = buildJsonLdExpandedDateTime(this)
            }
        }
        specificAccessPolicy?.run {
            resultEntity[AUTH_PROP_SAP] = buildJsonLdExpandedProperty(specificAccessPolicy)
        }

        rCanAdminUsers?.run {
            resultEntity[AUTH_REL_CAN_ADMIN] = rCanAdminUsers.map {
                buildJsonLdExpandedRelationship(it)
            }
        }
        rCanWriteUsers?.run {
            resultEntity[AUTH_REL_CAN_WRITE] = rCanWriteUsers.map {
                buildJsonLdExpandedRelationship(it)
            }
        }
        rCanReadUsers?.run {
            resultEntity[AUTH_REL_CAN_READ] = rCanReadUsers.map {
                buildJsonLdExpandedRelationship(it)
            }
        }
        return resultEntity
    }
}
