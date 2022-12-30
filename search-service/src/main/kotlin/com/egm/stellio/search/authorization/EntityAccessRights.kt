package com.egm.stellio.search.authorization

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.*
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
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedProperty
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedRelationship
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedDateTime
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedProperty
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

        fun serializeProperties(): ExpandedAttributePayload =
            buildExpandedRelationship(uri)
                .addSubAttribute(NGSILD_DATASET_ID_PROPERTY, buildNonReifiedProperty(datasetId))
                .addSubAttribute(AUTH_PROP_SUBJECT_INFO, buildExpandedProperty(subjectInfo))
    }

    fun serializeProperties(includeSysAttrs: Boolean): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()

        resultEntity[JSONLD_ID] = id.toString()
        resultEntity[JSONLD_TYPE] = types
        resultEntity[AUTH_PROP_RIGHT] = buildExpandedProperty(right.attributeName)

        if (includeSysAttrs) {
            resultEntity[NGSILD_CREATED_AT_PROPERTY] = buildNonReifiedDateTime(createdAt)
            modifiedAt?.run {
                resultEntity[NGSILD_MODIFIED_AT_PROPERTY] = buildNonReifiedDateTime(this)
            }
        }
        specificAccessPolicy?.run {
            resultEntity[AUTH_PROP_SAP] = buildExpandedProperty(this)
        }

        rCanAdminUsers?.run {
            resultEntity[AUTH_REL_CAN_ADMIN] = this.map {
                it.serializeProperties()
            }.flatten()
        }
        rCanWriteUsers?.run {
            resultEntity[AUTH_REL_CAN_WRITE] = this.map {
                it.serializeProperties()
            }.flatten()
        }
        rCanReadUsers?.run {
            resultEntity[AUTH_REL_CAN_READ] = this.map {
                it.serializeProperties()
            }.flatten()
        }
        return resultEntity
    }
}
