package com.egm.stellio.search.model

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue
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
    fun serializeProperties(): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        resultEntity[JSONLD_ID] = entityId.toString()
        resultEntity[JSONLD_TYPE] = types
        scopes?.run {
            resultEntity[NGSILD_SCOPE_PROPERTY] = this.map {
                mapOf(JSONLD_VALUE to it)
            }
        }
        specificAccessPolicy?.run {
            resultEntity[AuthContextModel.AUTH_PROP_SAP] = buildExpandedPropertyValue(this)
        }

        resultEntity[NGSILD_CREATED_AT_PROPERTY] = buildNonReifiedTemporalValue(createdAt)
        modifiedAt?.run {
            resultEntity[NGSILD_MODIFIED_AT_PROPERTY] = buildNonReifiedTemporalValue(this)
        }

        return resultEntity
    }
}
