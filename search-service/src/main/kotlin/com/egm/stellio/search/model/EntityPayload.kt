package com.egm.stellio.search.model

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.toExpandedDateTime
import java.net.URI
import java.time.ZonedDateTime

data class EntityPayload(
    val entityId: URI,
    val types: List<ExpandedTerm>,
    val createdAt: ZonedDateTime,
    val modifiedAt: ZonedDateTime? = null,
    // creation time contexts
    // FIXME only stored because needed to compact types at deletion time...
    val contexts: List<String>,
    val entityPayload: String? = null,
    val specificAccessPolicy: SpecificAccessPolicy? = null
) {
    fun serializeProperties(
        withSysAttrs: Boolean,
        withCompactTerms: Boolean = false,
        contexts: List<String> = emptyList()
    ): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        // as we are "manually" compacting the type, handle the case where there is just one of it
        // and convey it as a string (instead of a list of 1)
        if (withCompactTerms) {
            resultEntity[JSONLD_ID_TERM] = entityId.toString()
            resultEntity[JSONLD_TYPE_TERM] =
                types.map { compactTerm(it, contexts) }
                    .let { if (it.size > 1) it else it.first() }
            specificAccessPolicy?.run {
                resultEntity[AuthContextModel.AUTH_TERM_SAP] = specificAccessPolicy
            }
        } else {
            resultEntity[JSONLD_ID] = entityId.toString()
            resultEntity[JSONLD_TYPE] = types
            specificAccessPolicy?.run {
                resultEntity[AuthContextModel.AUTH_PROP_SAP] = specificAccessPolicy
            }
            if (withSysAttrs) {
                resultEntity[JsonLdUtils.NGSILD_CREATED_AT_PROPERTY] =
                    createdAt.toExpandedDateTime(JsonLdUtils.NGSILD_CREATED_AT_PROPERTY)

                modifiedAt?.run {
                    resultEntity[JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY] =
                        modifiedAt.toExpandedDateTime(JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY)
                }
            }
        }
        return resultEntity
    }
}
