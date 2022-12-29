package com.egm.stellio.search.authorization

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_RIGHT
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SUBJECT_INFO
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.JsonLdUtils.DATASET_ID_PREFIX
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.buildJsonLdExpandedDateTime
import com.egm.stellio.shared.util.JsonLdUtils.buildJsonLdExpandedProperty
import com.egm.stellio.shared.util.JsonLdUtils.buildJsonLdExpandedRelationship
import com.egm.stellio.shared.util.extractSub
import com.egm.stellio.shared.util.toUri
import java.net.URI
import java.time.ZonedDateTime

data class EntityAccessRights(
    val id: URI,
    val types: List<ExpandedTerm>,
    // right the current user has on the entity
    val right: AccessRight,
    val createdAt: ZonedDateTime,
    val modifiedAt: ZonedDateTime? = null,
    val specificAccessPolicy: AuthContextModel.SpecificAccessPolicy? = null,
    val rCanAdminUsers: List<SubjectRightInfo>? = null,
    val rCanWriteUsers: List<SubjectRightInfo>? = null,
    val rCanReadUsers: List<SubjectRightInfo>? = null
) {
    data class SubjectRightInfo(
        val uri: URI,
        val subjectInfo: Map<String, String>
    ) {
        val datasetId: URI = (DATASET_ID_PREFIX + uri.extractSub()).toUri()

        fun serializeProperties(): Map<String, Any> =
            buildJsonLdExpandedRelationship(uri)
                .plus(mapOf(NGSILD_DATASET_ID_PROPERTY to mapOf(JSONLD_ID to datasetId)))
                .plus(mapOf(AUTH_PROP_SUBJECT_INFO to buildJsonLdExpandedProperty(subjectInfo)))
    }

    fun serializeProperties(includeSysAttrs: Boolean): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()

        resultEntity[JSONLD_ID] = id.toString()
        resultEntity[JSONLD_TYPE] = types
        resultEntity[AUTH_PROP_RIGHT] = buildJsonLdExpandedProperty(right.attributeName)

        if (includeSysAttrs) {
            resultEntity[NGSILD_CREATED_AT_PROPERTY] = buildJsonLdExpandedDateTime(createdAt)
            modifiedAt?.run {
                resultEntity[NGSILD_MODIFIED_AT_PROPERTY] = buildJsonLdExpandedDateTime(this)
            }
        }
        specificAccessPolicy?.run {
            resultEntity[AUTH_PROP_SAP] = buildJsonLdExpandedProperty(this)
        }

        rCanAdminUsers?.run {
            resultEntity[AUTH_REL_CAN_ADMIN] = this.map {
                it.serializeProperties()
            }
        }
        rCanWriteUsers?.run {
            resultEntity[AUTH_REL_CAN_WRITE] = this.map {
                it.serializeProperties()
            }
        }
        rCanReadUsers?.run {
            resultEntity[AUTH_REL_CAN_READ] = this.map {
                it.serializeProperties()
            }
        }
        return resultEntity
    }
}
