package com.egm.stellio.search.authorization

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTHORIZATION_API_DEFAULT_CONTEXTS
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
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedProperty
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedRelationship
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedProperty
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
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

        fun serializeProperties(): ExpandedAttributeInstances =
            buildExpandedRelationship(uri)
                .addSubAttribute(NGSILD_DATASET_ID_PROPERTY, buildNonReifiedProperty(datasetId.toString()))
                .addSubAttribute(AUTH_PROP_SUBJECT_INFO, buildExpandedSubjectInfo(subjectInfo))
    }

    fun serializeProperties(): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()

        resultEntity[JSONLD_ID] = id.toString()
        resultEntity[JSONLD_TYPE] = types
        resultEntity[AUTH_PROP_RIGHT] = buildExpandedProperty(right.attributeName)

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

/**
 * Build the expanded subject info.
 *
 * For instance:
 *
 * "[
 *   {
 *     "@type": [
 *       "https://uri.etsi.org/ngsi-ld/Property"
 *     ],
 *     "https://uri.etsi.org/ngsi-ld/hasValue": [
 *       {
 *         "https://ontology.eglobalmark.com/authorization#kind": [
 *              {
 *                  "@value": "kind"
 *               }
 *         ],
 *         "https://ontology.eglobalmark.com/authorization#username": [
 *              {
 *                  "@value": "username"
 *              }
 *         ]
 *       }
 *     ]
 *   }
 * ]
 */
private fun buildExpandedSubjectInfo(value: Map<String, String>): ExpandedAttributeInstances =
    listOf(
        mapOf(
            JSONLD_TYPE to listOf(NGSILD_PROPERTY_TYPE.uri),
            NGSILD_PROPERTY_VALUE to listOf(
                expandJsonLdFragment(value, AUTHORIZATION_API_DEFAULT_CONTEXTS).mapValues {
                    it.value
                }
            )
        )
    )
