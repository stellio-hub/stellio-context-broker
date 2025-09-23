package com.egm.stellio.search.authorization.permission.model

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.Scope
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonIgnore
import java.net.URI

data class TargetAsset(
    val id: URI? = null,
    @JsonFormat(with = [JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY])
    val types: List<ExpandedTerm>? = null,
    @JsonFormat(with = [JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY])
    val scopes: List<Scope>? = null
) {
    fun expand(contexts: List<String>): TargetAsset =
        this.copy(
            types = types?.map { expandJsonLdTerm(it, contexts) },
        )

    fun compact(contexts: List<String>): TargetAsset =
        this.copy(
            types = types?.map { compactTerm(it, contexts) },
        )

    fun validate(): Either<APIException, Unit> =
        if (id == null && types == null && scopes == null) {
            BadRequestDataException("You must specify a target id, types or scopes").left()
        } else if (id != null && (types != null || scopes != null)) {
            BadRequestDataException("You can't target an id and types or scopes").left()
        } else { Unit.right() }

    @JsonIgnore
    fun isTargetingEntity() = id != null

    companion object {
        fun deserialize(
            input: Map<String, Any>,
            contexts: List<String>
        ): Either<APIException, TargetAsset> =
            runCatching {
                deserializeAs<TargetAsset>(serializeObject(input.plus(JSONLD_CONTEXT_KW to contexts)))
                    .expand(contexts)
            }.fold(
                { it.right() },
                { it.toAPIException("Failed to parse target caused by : ${it.message}").left() }
            )
    }
}
