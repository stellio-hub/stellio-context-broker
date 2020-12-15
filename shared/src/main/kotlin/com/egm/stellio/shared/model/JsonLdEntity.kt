package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import org.springframework.http.MediaType

typealias CompactedJsonLdEntity = Map<String, Any>

data class JsonLdEntity(
    val properties: Map<String, Any>,
    val contexts: List<String>
) {
    fun compact(mediaType: MediaType = JSON_LD_MEDIA_TYPE): CompactedJsonLdEntity =
        if (mediaType == MediaType.APPLICATION_JSON)
            JsonLdProcessor.compact(properties, mapOf("@context" to contexts), JsonLdOptions())
                .minus(JsonLdUtils.JSONLD_CONTEXT)
        else
            JsonLdProcessor.compact(properties, mapOf("@context" to contexts), JsonLdOptions())

    fun containsAnyOf(expandedAttributes: Set<String>): Boolean =
        expandedAttributes.isEmpty() || properties.keys.any { expandedAttributes.contains(it) }

    // FIXME kinda hacky but we often just need the id or type... how can it be improved?
    val id by lazy {
        (properties[JSONLD_ID] ?: InternalErrorException("Could not extract id from JSON-LD entity")) as String
    }
    val type by lazy {
        val types =
            (properties[JSONLD_TYPE] ?: InternalErrorException("Could not extract type from JSON-LD entity"))
                as List<String>
        types[0]
    }
}
