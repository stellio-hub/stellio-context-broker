package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor

data class JsonLdEntity(
    val properties: Map<String, Any>,
    val contexts: List<String>
) {
    fun compact(): Map<String, Any> =
        JsonLdProcessor.compact(properties, mapOf("@context" to contexts), JsonLdOptions())

    // FIXME kinda hacky but we often just need the id or type... how can it be improved?
    val id by lazy {
        (properties[JSONLD_ID] ?: InternalErrorException("Could not extract id from JSON-LD entity")) as String
    }
    val type by lazy {
        ((properties[JSONLD_TYPE] ?: InternalErrorException("Could not extract type from JSON-LD entity")) as List<String>)[0]
    }
}
