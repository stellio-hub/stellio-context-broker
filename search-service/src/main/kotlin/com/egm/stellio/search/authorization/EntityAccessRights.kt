package com.egm.stellio.search.authorization

import com.egm.stellio.shared.model.ExpandedAttributeInstances
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.addNonReifiedProperty
import com.egm.stellio.shared.model.addSubAttribute
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
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyMapValue
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedRelationshipValue
import com.egm.stellio.shared.util.extractSub
import com.egm.stellio.shared.util.toUri
import java.net.URI

data class EntityAccessRights(
    val id: URI,
    val types: List<ExpandedTerm>,
    // right the current user has on the entity
    val right: AccessRight,
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

        suspend fun serializeProperties(contexts: List<String>): ExpandedAttributeInstances =
            buildExpandedRelationshipValue(uri)
                .addNonReifiedProperty(NGSILD_DATASET_ID_PROPERTY, datasetId.toString())
                .addSubAttribute(
                    AUTH_PROP_SUBJECT_INFO,
                    buildExpandedPropertyMapValue(subjectInfo, contexts)
                )
    }

    suspend fun serializeProperties(contexts: List<String>): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()

        resultEntity[JSONLD_ID] = id.toString()
        resultEntity[JSONLD_TYPE] = types
        resultEntity[AUTH_PROP_RIGHT] = buildExpandedPropertyValue(right.attributeName)

        specificAccessPolicy?.run {
            resultEntity[AUTH_PROP_SAP] = buildExpandedPropertyValue(this)
        }

        rCanAdminUsers?.run {
            resultEntity[AUTH_REL_CAN_ADMIN] = this.map {
                it.serializeProperties(contexts)
            }.flatten()
        }
        rCanWriteUsers?.run {
            resultEntity[AUTH_REL_CAN_WRITE] = this.map {
                it.serializeProperties(contexts)
            }.flatten()
        }
        rCanReadUsers?.run {
            resultEntity[AUTH_REL_CAN_READ] = this.map {
                it.serializeProperties(contexts)
            }.flatten()
        }
        return resultEntity
    }
}
