package com.egm.stellio.shared.model

import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor

class ExpandedEntity private constructor(
    val attributes: Map<String, Any>,
    val contexts: List<String>
) {

    companion object {
        operator fun invoke(attributes: Map<String, Any>, contexts: List<String>): ExpandedEntity {
            if (!attributes.containsKey("@id"))
                throw BadRequestDataException("The provided NGSI-LD entity does not contain an id property")

            if (!attributes.containsKey("@type"))
                throw BadRequestDataException("The provided NGSI-LD entity does not contain a type property")

            return ExpandedEntity(attributes, contexts)
        }
    }

    val id = attributes["@id"]!! as String
    val type = (attributes["@type"]!! as List<String>)[0]

    fun compact(): Map<String, Any> =
        JsonLdProcessor.compact(attributes, mapOf("@context" to contexts), JsonLdOptions())
}
