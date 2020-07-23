package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.AttributeType
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_ID
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor

class ExpandedEntity private constructor(
    val rawJsonLdProperties: Map<String, Any>,
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

    val id = rawJsonLdProperties[NGSILD_ENTITY_ID]!! as String
    val type = (rawJsonLdProperties[NGSILD_ENTITY_TYPE]!! as List<String>)[0]
    val relationships by lazy { getAttributesOfType(NGSILD_RELATIONSHIP_TYPE) as Map<String, List<Map<String, List<Any>>>> }
    val properties by lazy { getAttributesOfType(NGSILD_PROPERTY_TYPE) as Map<String, List<Map<String, List<Any>>>> }
    val geoProperties by lazy { getAttributesOfType(NGSILD_GEOPROPERTY_TYPE) as Map<String, Map<String, List<Any>>> }
    val attributes by lazy { initAttributesWithoutTypeAndId() }

    fun compact(): Map<String, Any> =
        JsonLdProcessor.compact(rawJsonLdProperties, mapOf("@context" to contexts), JsonLdOptions())

    private fun getAttributesOfType(type: AttributeType): Any =
        when (type) {
            NGSILD_GEOPROPERTY_TYPE -> attributes.mapValues {
                NgsiLdParsingUtils.expandValueAsMap(it.value)
            }.filter {
                NgsiLdParsingUtils.isAttributeOfType(it.value, type)
            }
            else -> attributes.mapValues {
                NgsiLdParsingUtils.expandValueAsListOfMap(it.value)
            }
            .filter {
                !NGSILD_ENTITY_CORE_MEMBERS.contains(it.key) &&
                    NgsiLdParsingUtils.isValidAttribute(it.key, it.value)
            }
            .filter {
                NgsiLdParsingUtils.isAttributeOfType(it.value, type)
            }
        }

    private fun initAttributesWithoutTypeAndId(): Map<String, Any> {
        val idAndTypeKeys = listOf(NGSILD_ENTITY_ID, NGSILD_ENTITY_TYPE)
        return rawJsonLdProperties.filterKeys {
            !idAndTypeKeys.contains(it)
        }
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
        return relationships.flatMap { relationship ->
            relationship.value.map {
                NgsiLdParsingUtils.getRelationshipObjectId(it)
            }
        }.plus(getLinkedEntitiesByRelationship(relationships))
    }

    private fun getLinkedEntitiesIdsByProperties(): List<String> {
        return getLinkedEntitiesByProperty(properties)
    }

    private fun getLinkedEntitiesByRelationship(attributes: Map<String, List<Map<String, List<Any>>>>): List<String> {
        return attributes.flatMap { attribute ->
            attribute.value.flatMap { instance ->
                instance.map {
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

    private fun getLinkedEntitiesByProperty(attributes: Map<String, List<Map<String, List<Any>>>>): List<String> {
        return attributes.flatMap { attribute ->
            attribute.value.flatMap { instance ->
                instance.map {
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

    fun checkAttributesHaveAtMostOneDefaultInstance() {
        properties.findAttributeWithMoreThanOneDefaultInstance()?.let { throw BadRequestDataException("Property ${it.extractShortTypeFromExpanded()} can't have more than one default instance") }
        relationships.findAttributeWithMoreThanOneDefaultInstance()?.let { throw BadRequestDataException("Relationship ${it.extractShortTypeFromExpanded()} can't have more than one default instance") }
    }

    fun checkAttributesHaveUniqueDatasetId() {
        properties.findAttributeWithDuplicatedDatasetId()?.let { throw BadRequestDataException("Property ${it.extractShortTypeFromExpanded()} can't have duplicated datasetId") }
        relationships.findAttributeWithDuplicatedDatasetId()?.let { throw BadRequestDataException("Relationship ${it.extractShortTypeFromExpanded()} can't have duplicated datasetId") }
    }

    private fun Map<String, List<Map<String, List<Any>>>>.findAttributeWithMoreThanOneDefaultInstance(): String? {
        this.forEach { attribute ->
            if (attribute.value.count { !it.containsKey(NGSILD_DATASET_ID_PROPERTY) } > 1)
                return attribute.key
        }
        return null
    }

    private fun Map<String, List<Map<String, List<Any>>>>.findAttributeWithDuplicatedDatasetId(): String? {
        this.forEach { attribute ->
            val datasetIds = attribute.value.map {
                val datasetId = it[NGSILD_DATASET_ID_PROPERTY]?.get(0) as Map<String, String>?
                datasetId?.get(NGSILD_ENTITY_ID)
            }
            if (datasetIds.toSet().count() != datasetIds.count())
                return attribute.key
        }
        return null
    }
}