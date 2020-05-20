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

    val id = attributes[NgsiLdParsingUtils.NGSILD_ENTITY_ID]!! as String
    val type = (attributes[NgsiLdParsingUtils.NGSILD_ENTITY_TYPE]!! as List<String>)[0]
    val relationships by lazy { getAttributesOfType(NGSILD_RELATIONSHIP_TYPE) }
    val properties by lazy { getAttributesOfType(NGSILD_PROPERTY_TYPE) }
    val attributesWithoutTypeAndId by lazy {
        val idAndTypeKeys = listOf(NgsiLdParsingUtils.NGSILD_ENTITY_ID, NgsiLdParsingUtils.NGSILD_ENTITY_TYPE)
        attributes.filterKeys {
            !idAndTypeKeys.contains(it)
        }
    }

    fun compact(): Map<String, Any> =
        JsonLdProcessor.compact(attributes, mapOf("@context" to contexts), JsonLdOptions())

    private fun getAttributesOfType(type: AttributeType): Map<String, Map<String, List<Any>>> =
        attributesWithoutTypeAndId.mapValues {
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
        return relationships.map {
            NgsiLdParsingUtils.getRelationshipObjectId(it.value)
        }.plus(getLinkedEntitiesByAttribute(relationships))
    }

    private fun getLinkedEntitiesIdsByProperties(): List<String> {
        return getLinkedEntitiesByAttribute(properties)
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