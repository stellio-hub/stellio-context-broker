package com.egm.stellio.search.authorization.permission.model

import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.fasterxml.jackson.annotation.JsonFormat
import java.net.URI

/**
 * EntityInfo type as defined in 5.2.8
 */
data class TargetAsset(
    @JsonFormat(with = [JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY])
    val types: List<String>? = null, // not implemented yet
    val scope: String? = null, // not implemented yet
    val id: URI? = null, // not compatible with scope
) {
    fun expand(contexts: List<String>): TargetAsset =
        this.copy(
            types = types?.map { expandJsonLdTerm(it, contexts) },
        )

    fun compact(contexts: List<String>): TargetAsset =
        this.copy(
            types = types?.map { compactTerm(it, contexts) },
        )
}
