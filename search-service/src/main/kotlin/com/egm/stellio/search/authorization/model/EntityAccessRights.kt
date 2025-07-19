package com.egm.stellio.search.authorization.model

import com.egm.stellio.shared.model.ExpandedAttributeInstances
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_IRI
import com.egm.stellio.shared.model.addNonReifiedProperty
import com.egm.stellio.shared.model.addSubAttribute
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_IS_DELETED
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_RIGHT
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SUBJECT_INFO
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_IS_OWNER
import com.egm.stellio.shared.util.AuthContextModel.DATASET_ID_PREFIX
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyMapValue
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedRelationshipValue
import com.egm.stellio.shared.util.extractSub
import com.egm.stellio.shared.util.toUri
import java.net.URI

data class EntityAccessRights(
    val id: URI,
    val types: List<ExpandedTerm>,
    val isDeleted: Boolean = false,
    // right the current user has on the entity
    val right: AccessRight,
    val specificAccessPolicy: AuthContextModel.SpecificAccessPolicy? = null,
    val canAdmin: List<SubjectRightInfo>? = null,
    val canWrite: List<SubjectRightInfo>? = null,
    val canRead: List<SubjectRightInfo>? = null,
    val owner: SubjectRightInfo? = null
) {
    data class SubjectRightInfo(
        val uri: URI,
        val subjectInfo: Map<String, String>
    ) {
        val datasetId: URI = (DATASET_ID_PREFIX + uri.extractSub()).toUri()

        suspend fun serializeProperties(contexts: List<String>): ExpandedAttributeInstances =
            buildExpandedRelationshipValue(uri)
                .addNonReifiedProperty(NGSILD_DATASET_ID_IRI, datasetId.toString())
                .addSubAttribute(
                    AUTH_PROP_SUBJECT_INFO,
                    buildExpandedPropertyMapValue(subjectInfo, contexts)
                )
    }

    suspend fun serializeProperties(contexts: List<String>): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()

        resultEntity[JSONLD_ID_KW] = id.toString()
        resultEntity[JSONLD_TYPE_KW] = types
        if (isDeleted)
            resultEntity[AUTH_PROP_IS_DELETED] = buildExpandedPropertyValue(true)
        resultEntity[AUTH_PROP_RIGHT] = buildExpandedPropertyValue(right.attributeName)

        specificAccessPolicy?.run {
            resultEntity[AUTH_PROP_SAP] = buildExpandedPropertyValue(this)
        }

        canAdmin?.run {
            resultEntity[AUTH_REL_CAN_ADMIN] = this.map {
                it.serializeProperties(contexts)
            }.flatten()
        }
        canWrite?.run {
            resultEntity[AUTH_REL_CAN_WRITE] = this.map {
                it.serializeProperties(contexts)
            }.flatten()
        }
        canRead?.run {
            resultEntity[AUTH_REL_CAN_READ] = this.map {
                it.serializeProperties(contexts)
            }.flatten()
        }
        owner?.run {
            resultEntity[AUTH_REL_IS_OWNER] = this.serializeProperties(contexts)
        }

        return resultEntity
    }
}
