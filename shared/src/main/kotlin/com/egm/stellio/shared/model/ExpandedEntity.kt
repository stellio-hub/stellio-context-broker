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
     * Gets linked entities ids.
     * Entities can be linked either by a relation or a property.
     *
     * @return a subset of [entities]
     */
    fun getLinkedEntitiesIds(): List<String> =
        getLinkedEntitiesIdsByProperties().plus(getLinkedEntitiesIdsByRelations())

    private fun getLinkedEntitiesIdsByRelations(): List<String> {
        val relationships = getAttributeOfType(NGSILD_RELATIONSHIP_TYPE)

        return relationships.map {
            NgsiLdParsingUtils.getRelationshipObjectId(it.value)
        }.plus(getLinkedEntitiesByAttribute(relationships))
    }

    private fun getLinkedEntitiesIdsByProperties(): List<String> {
        val attributes = getAttributeOfType(NGSILD_PROPERTY_TYPE)

        return getLinkedEntitiesByAttribute(attributes)
    }

    private fun getLinkedEntitiesByAttribute(attributes: Map<String, Map<String, List<Any>>>): List<String> {
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