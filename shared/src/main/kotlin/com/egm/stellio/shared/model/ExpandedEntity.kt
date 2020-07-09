package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.AttributeType
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_ID
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_RELATIONSHIP_TYPE
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor

class ExpandedEntity private constructor(
    val rawJsonLdProperties: Map<String, Any>,
    val attributes: Map<String, Any>,
    val id: String,
    val type: String,
    val properties: Map<String, List<Map<String, List<Any>>>>,
    val relationships: Map<String, Map<String, List<Any>>>,
    val geoProperties: Map<String, Map<String, List<Any>>>,
    val contexts: List<String>
) {
    companion object {
        operator fun invoke(rawJsonLdProperties: Map<String, Any>, contexts: List<String>): ExpandedEntity {
            if (!rawJsonLdProperties.containsKey("@id"))
                throw BadRequestDataException("The provided NGSI-LD entity does not contain an id property")

            if (!rawJsonLdProperties.containsKey("@type"))
                throw BadRequestDataException("The provided NGSI-LD entity does not contain a type property")

            val attributes = initAttributesWithoutTypeAndId(rawJsonLdProperties)
            val properties = getAttributesOfType(attributes, NGSILD_PROPERTY_TYPE) as Map<String, List<Map<String, List<Any>>>>

            if (!properties.haveAtMostOneDefaultInstance())
                throw BadRequestDataException("Properties can't have more than one default instance")

            if (!properties.haveUniqueDatasetId())
                throw BadRequestDataException("Properties can't have duplicated datasetId")

            val relationships = getAttributesOfType(attributes, NGSILD_RELATIONSHIP_TYPE) as Map<String, Map<String, List<Any>>>
            val geoProperties = getAttributesOfType(attributes, NGSILD_GEOPROPERTY_TYPE) as Map<String, Map<String, List<Any>>>
            val id = rawJsonLdProperties[NGSILD_ENTITY_ID]!! as String
            val type = (rawJsonLdProperties[NGSILD_ENTITY_TYPE]!! as List<String>)[0]
            return ExpandedEntity(rawJsonLdProperties, attributes, id, type, properties, relationships, geoProperties, contexts)
        }

        private fun initAttributesWithoutTypeAndId(rawJsonLdProperties: Map<String, Any>): Map<String, Any> {
            val idAndTypeKeys = listOf(NGSILD_ENTITY_ID, NGSILD_ENTITY_TYPE)
            return rawJsonLdProperties.filterKeys {
                !idAndTypeKeys.contains(it)
            }
        }

        private fun getAttributesOfType(attributes: Map<String, Any>, type: AttributeType): Any =
            when (type) {
                NGSILD_PROPERTY_TYPE -> attributes.mapValues {
                        NgsiLdParsingUtils.expandValueAsListOfMap(it.value)
                    }
                    .filter {
                        NgsiLdParsingUtils.isValidAttribute(it.value)
                    }
                    .filter {
                        NgsiLdParsingUtils.isAttributeOfType(it.value, type)
                    }
                else -> attributes.mapValues {
                        NgsiLdParsingUtils.expandValueAsMap(it.value)
                    }
                    .filter {
                        NgsiLdParsingUtils.isAttributeOfType(it.value, type)
                    }
            }

        private fun Map<String, List<Map<String, List<Any>>>>.haveAtMostOneDefaultInstance(): Boolean {
            return this.all { property ->
                property.value.count { !it.containsKey(NGSILD_DATASET_ID_PROPERTY) } < 2
            }
        }

        private fun Map<String, List<Map<String, List<Any>>>>.haveUniqueDatasetId(): Boolean {
            return this.all { property ->
                val datasetIds = property.value.map {
                    val datasetId = it[NGSILD_DATASET_ID_PROPERTY]?.get(0) as Map<String, String>?
                    datasetId?.get(NGSILD_ENTITY_ID)
                }

                datasetIds.toSet().count() == datasetIds.count()
            }
        }
    }

    fun compact(): Map<String, Any> =
        JsonLdProcessor.compact(rawJsonLdProperties, mapOf("@context" to contexts), JsonLdOptions())

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
        }.plus(getLinkedEntitiesByRelationship(relationships))
    }

    private fun getLinkedEntitiesIdsByProperties(): List<String> {
        return getLinkedEntitiesByProperty(properties)
    }

    private fun getLinkedEntitiesByRelationship(attributes: Map<String, Map<String, List<Any>>>): List<String> {
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
}
