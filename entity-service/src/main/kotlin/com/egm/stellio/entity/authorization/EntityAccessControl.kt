package com.egm.stellio.entity.authorization

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_RIGHT
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.JsonLdUtils
import java.net.URI
import java.time.ZonedDateTime

data class EntityAccessControl(
    val id: URI,
    val type: List<ExpandedTerm>,
    val datasetId: URI,
    val createdAt: ZonedDateTime,
    val modifiedAt: ZonedDateTime? = null,
    val rCanAdminUsers: List<URI>? = null,
    val rCanWriteUsers: List<URI>? = null,
    val rCanReadUsers: List<URI>? = null,
    val right: AccessRight,
    val specificAccessPolicy: AuthContextModel.SpecificAccessPolicy? = null
) {
    fun serializeProperties(includeSysAttrs: Boolean): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()

        resultEntity[JsonLdUtils.JSONLD_ID] = id.toString()
        resultEntity[JsonLdUtils.JSONLD_TYPE] = type
        datasetId.run {
            resultEntity[JsonLdUtils.NGSILD_DATASET_ID_PROPERTY] = mapOf(
                JsonLdUtils.JSONLD_ID to this.toString()
            )
        }
        resultEntity[AUTH_PROP_RIGHT] = JsonLdUtils.buildJsonLdExpandedProperty(right.attributeName)

        if (includeSysAttrs) {
            resultEntity[JsonLdUtils.NGSILD_CREATED_AT_PROPERTY] = JsonLdUtils.buildJsonLdExpandedDateTime(createdAt)
            modifiedAt?.run {
                resultEntity[JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY] = JsonLdUtils.buildJsonLdExpandedDateTime(this)
            }
        }
        specificAccessPolicy?.run {
            resultEntity[AUTH_PROP_SAP] = JsonLdUtils.buildJsonLdExpandedProperty(specificAccessPolicy)
        }

        rCanAdminUsers?.run {
            resultEntity[AUTH_REL_CAN_ADMIN] = rCanAdminUsers.map {
                JsonLdUtils.buildJsonLdExpandedRelationship(it)
            }
        }
        rCanWriteUsers?.run {
            resultEntity[AUTH_REL_CAN_WRITE] = rCanWriteUsers.map {
                JsonLdUtils.buildJsonLdExpandedRelationship(it)
            }
        }
        rCanReadUsers?.run {
            resultEntity[AUTH_REL_CAN_READ] = rCanReadUsers.map {
                JsonLdUtils.buildJsonLdExpandedRelationship(it)
            }
        }
        return resultEntity
    }
}
