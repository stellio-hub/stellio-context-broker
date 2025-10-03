package com.egm.stellio.search.entity.model

import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.common.util.deserializeExpandedPayload
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_CREATED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_DELETED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_SCOPE_IRI
import com.egm.stellio.shared.model.Scope
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue
import io.r2dbc.postgresql.codec.Json
import java.net.URI
import java.time.ZonedDateTime

data class Entity(
    val entityId: URI,
    val types: List<ExpandedTerm>,
    val scopes: List<Scope>? = null,
    val createdAt: ZonedDateTime,
    val modifiedAt: ZonedDateTime = createdAt,
    val deletedAt: ZonedDateTime? = null,
    val payload: Json,
    val specificAccessPolicy: Action? = null
) {
    fun serializeProperties(): Map<String, Any> {
        val resultEntity = mutableMapOf<String, Any>()
        resultEntity[JSONLD_ID_KW] = entityId.toString()
        resultEntity[JSONLD_TYPE_KW] = types
        scopes?.run {
            resultEntity[NGSILD_SCOPE_IRI] = this.map {
                mapOf(JSONLD_VALUE_KW to it)
            }
        }
        specificAccessPolicy?.run {
            resultEntity[AuthContextModel.AUTH_PROP_SAP] = buildExpandedPropertyValue(this)
        }

        resultEntity[NGSILD_CREATED_AT_IRI] = buildNonReifiedTemporalValue(createdAt)
        resultEntity[NGSILD_MODIFIED_AT_IRI] = buildNonReifiedTemporalValue(modifiedAt)

        return resultEntity
    }

    fun toExpandedDeletedEntity(
        deletedAt: ZonedDateTime
    ): ExpandedEntity =
        ExpandedEntity(
            members = mapOf(
                JSONLD_ID_KW to entityId,
                JSONLD_TYPE_KW to types,
                NGSILD_CREATED_AT_IRI to buildNonReifiedTemporalValue(createdAt),
                NGSILD_MODIFIED_AT_IRI to buildNonReifiedTemporalValue(modifiedAt),
                NGSILD_DELETED_AT_IRI to buildNonReifiedTemporalValue(deletedAt),
            )
        )

    fun toExpandedEntity(): ExpandedEntity =
        ExpandedEntity(this.payload.deserializeExpandedPayload())
}
