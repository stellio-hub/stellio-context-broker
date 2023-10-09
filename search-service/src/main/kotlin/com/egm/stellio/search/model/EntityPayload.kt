package com.egm.stellio.search.model

import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.ExpandedTerm
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedProperty
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedDateTime
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import io.r2dbc.postgresql.codec.Json
import java.net.URI
import java.time.ZonedDateTime

data class EntityPayload(
    val entityId: URI,
    val types: List<ExpandedTerm>,
    val scopes: List<String>? = null,
    val createdAt: ZonedDateTime,
    val modifiedAt: ZonedDateTime? = null,
    // creation time contexts
    // FIXME only stored because needed to compact types at deletion time...
    val contexts: List<String>,
    val payload: Json,
    val specificAccessPolicy: SpecificAccessPolicy? = null
) {
    fun serializeProperties(
        withSysAttrs: Boolean,
        withCompactTerms: Boolean = false,
        contexts: List<String> = emptyList()
    ): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        // "manual" compaction is used for temporal entities where temporalValues representation and aggregations
        // are badly handled by the normal JSON-LD compaction process (lists of lists are lost mainly)
        if (withCompactTerms) {
            resultEntity[JSONLD_ID_TERM] = entityId.toString()
            // as we are "manually" compacting the type, handle the case where there is just one of it
            // and convey it as a string (instead of a list of 1)
            resultEntity[JSONLD_TYPE_TERM] =
                types.map { compactTerm(it, contexts) }
                    .let { if (it.size > 1) it else it.first() }
            scopes?.run {
                resultEntity[NGSILD_SCOPE_TERM] = this
            }
            specificAccessPolicy?.run {
                resultEntity[AuthContextModel.AUTH_TERM_SAP] = mapOf(
                    JSONLD_TYPE_TERM to "Property",
                    JSONLD_VALUE_TERM to this
                )
            }
        } else {
            resultEntity[JSONLD_ID] = entityId.toString()
            resultEntity[JSONLD_TYPE] = types
            scopes?.run {
                resultEntity[NGSILD_SCOPE_PROPERTY] = this.map {
                    mapOf(JSONLD_VALUE to it)
                }
            }
            specificAccessPolicy?.run {
                resultEntity[AuthContextModel.AUTH_PROP_SAP] = buildExpandedProperty(this)
            }
            if (withSysAttrs) {
                resultEntity[NGSILD_CREATED_AT_PROPERTY] = buildNonReifiedDateTime(createdAt)

                modifiedAt?.run {
                    resultEntity[NGSILD_MODIFIED_AT_PROPERTY] = buildNonReifiedDateTime(this)
                }
            }
        }
        return resultEntity
    }
}
