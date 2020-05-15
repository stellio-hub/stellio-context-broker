package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.AttributeType
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_RELATIONSHIP_TYPE
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

    private fun getAttributeOfType(type: AttributeType) =
        attributes.filterKeys {
            !listOf(NgsiLdParsingUtils.NGSILD_ENTITY_ID, NgsiLdParsingUtils.NGSILD_ENTITY_TYPE).contains(it)
        }.mapValues {
            NgsiLdParsingUtils.expandValueAsMap(it.value)
        }.filter {
            NgsiLdParsingUtils.isAttributeOfType(it.value, type)
        }

    /**
     * Gets relationships of [entity]
     * Relationships can either be direct, or from a property.
     *
     * @return a subset of [entities]
     */
    fun getRelationships(): List<String> =
        getRelationshipsFromProperties().plus(getRelationshipsFromRelations())

    private fun getRelationshipsFromRelations(): List<String> {
        val relationships = getAttributeOfType(NGSILD_RELATIONSHIP_TYPE)

        return relationships.map {
            NgsiLdParsingUtils.getRelationshipObjectId(it.value)
        }.plus(getRelationshipFromAttributes(relationships))
    }

    private fun getRelationshipsFromProperties(): List<String> {
        val attributes = getAttributeOfType(NGSILD_PROPERTY_TYPE)

        return getRelationshipFromAttributes(attributes)
    }

    private fun getRelationshipFromAttributes(attributes: Map<String, Map<String, List<Any>>>): List<String> {
        return attributes.flatMap { attribute ->
            attribute.value
                .map {
                    it.value[0]
                }.filterIsInstance<Map<String, List<Any>>>()
                .filter {
                    NgsiLdParsingUtils.isAttributeOfType(it, NGSILD_RELATIONSHIP_TYPE)
                }.map {
                    NgsiLdParsingUtils.getRelationshipObjectId(it)
                }
        }
    }
}