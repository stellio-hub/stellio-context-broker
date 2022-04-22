package com.egm.stellio.entity.authorization

import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_RIGHT
import com.egm.stellio.shared.util.JsonLdUtils
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
    var modifiedAt: ZonedDateTime? = null,
    val right: AccessRight,
    val specificAccessPolicy: AuthContextModel.SpecificAccessPolicy? = null
) {
    fun serializeProperties(includeSysAttrs: Boolean): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        resultEntity[JsonLdUtils.JSONLD_ID] = id.toString()
        resultEntity[JsonLdUtils.JSONLD_TYPE] = type

        if (includeSysAttrs) {
            resultEntity[JsonLdUtils.NGSILD_CREATED_AT_PROPERTY] = mapOf(
                JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_DATE_TIME_TYPE,
                JsonLdUtils.JSONLD_VALUE_KW to createdAt.toNgsiLdFormat()
            )
            modifiedAt?.run {
                resultEntity[JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY] = mapOf(
                    JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_DATE_TIME_TYPE,
                    JsonLdUtils.JSONLD_VALUE_KW to this.toNgsiLdFormat()
                )
            }
        }

        resultEntity[AUTH_PROP_RIGHT] = mutableMapOf(
            JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_PROPERTY_TYPE.uri,
            JsonLdUtils.NGSILD_PROPERTY_VALUE to mapOf(
                JsonLdUtils.JSONLD_VALUE_KW to right.attributeName
            )
        )

        specificAccessPolicy?.run {
            resultEntity[AuthContextModel.AUTH_PROP_SAP] = mutableMapOf(
                JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_PROPERTY_TYPE.uri,
                JsonLdUtils.NGSILD_PROPERTY_VALUE to mapOf(JsonLdUtils.JSONLD_VALUE_KW to specificAccessPolicy)
            )
        }

        rCanAdminUsers?.run {
            resultEntity[AccessRight.R_CAN_WRITE.attributeName] = rCanAdminUsers.map {
                mutableMapOf(
                    JsonLdUtils.NGSILD_RELATIONSHIP_TYPE to JsonLdUtils.NGSILD_RELATIONSHIP_TYPE.uri,
                    JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(JsonLdUtils.JSONLD_ID to it)
                )
            }
        }
        rCanWriteUsers?.run {
            resultEntity[AccessRight.R_CAN_WRITE.attributeName] = rCanWriteUsers.map {
                mutableMapOf(
                    JsonLdUtils.NGSILD_RELATIONSHIP_TYPE to JsonLdUtils.NGSILD_RELATIONSHIP_TYPE.uri,
                    JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(JsonLdUtils.JSONLD_ID to it)
                )
            }
        }
        rCanReadUsers?.run {
            resultEntity[AccessRight.R_CAN_WRITE.attributeName] = rCanReadUsers.map {
                mutableMapOf(
                    JsonLdUtils.NGSILD_RELATIONSHIP_TYPE to JsonLdUtils.NGSILD_RELATIONSHIP_TYPE.uri,
                    JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(JsonLdUtils.JSONLD_ID to it)
                )
            }
        }

        return resultEntity
    }
}
