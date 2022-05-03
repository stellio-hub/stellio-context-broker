package com.egm.stellio.entity.authorization

import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_RIGHT
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.constructJsonLdDateTime
import com.egm.stellio.shared.util.toNgsiLdFormat
import java.net.URI
import java.time.ZonedDateTime

data class EntityAccessControl(
    val id: URI,
    val type: List<String>,
    val createdAt: ZonedDateTime,
    val rCanAdminUsers: List<URI>? = null,
    val rCanWriteUsers: List<URI>? = null,
    val rCanReadUsers: List<URI>? = null,
    val modifiedAt: ZonedDateTime? = null,
    val right: AccessRight,
    val specificAccessPolicy: AuthContextModel.SpecificAccessPolicy? = null
) {
    fun serializeProperties(includeSysAttrs: Boolean): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()

        resultEntity[JsonLdUtils.JSONLD_ID] = id.toString()
        resultEntity[JsonLdUtils.JSONLD_TYPE] = type
        resultEntity[AUTH_PROP_RIGHT] = JsonLdUtils.constructJsonLdProperty(right.attributeName)

        if (includeSysAttrs) {
            resultEntity[JsonLdUtils.NGSILD_CREATED_AT_PROPERTY] = constructJsonLdDateTime(createdAt.toNgsiLdFormat())
            modifiedAt?.run {
                resultEntity[JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY] = constructJsonLdDateTime(this.toNgsiLdFormat())
            }
        }
        specificAccessPolicy?.run {
            resultEntity[AUTH_PROP_SAP] = JsonLdUtils.constructJsonLdProperty(specificAccessPolicy)
        }

        rCanAdminUsers?.run {
            resultEntity[AUTH_REL_CAN_ADMIN] = rCanAdminUsers.map {
                JsonLdUtils.constructJsonLdRelationship(it)
            }
        }
        rCanWriteUsers?.run {
            resultEntity[AUTH_REL_CAN_WRITE] = rCanWriteUsers.map {
                JsonLdUtils.constructJsonLdRelationship(it)
            }
        }
        rCanReadUsers?.run {
            resultEntity[AUTH_REL_CAN_READ] = rCanReadUsers.map {
                JsonLdUtils.constructJsonLdRelationship(it)
            }
        }
        return resultEntity
    }
}
