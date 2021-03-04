package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.util.*
import org.springframework.stereotype.Service
import java.net.URI

typealias SimplifiedTemporalAttribute = Map<String, Any>

@Service
class TemporalEntityService {

    fun buildTemporalEntity(
        entityId: URI,
        attributeAndResultsMap: Map<TemporalEntityAttribute, List<AttributeInstanceResult>>,
        temporalQuery: TemporalQuery,
        contexts: List<String>,
        withTemporalValues: Boolean
    ): CompactedJsonLdEntity {
        val temporalAttributes = buildTemporalAttributes(
            attributeAndResultsMap,
            temporalQuery,
            contexts,
            withTemporalValues
        )

        return mapOf(
            "id" to entityId,
            "type" to JsonLdUtils.compactTerm(attributeAndResultsMap.keys.first().type, contexts)
        ).plus(temporalAttributes)
    }

    private fun buildTemporalAttributes(
        attributeAndResultsMap: Map<TemporalEntityAttribute, List<AttributeInstanceResult>>,
        temporalQuery: TemporalQuery,
        contexts: List<String>,
        withTemporalValues: Boolean
    ):
        Map<String, Any> {
            return if (withTemporalValues || temporalQuery.timeBucket != null) {
                val attributes = buildAttributesSimplifiedRepresentation(attributeAndResultsMap)
                mergeSimplifiedTemporalAttributesOnAttributeName(attributes)
                    .mapKeys { JsonLdUtils.compactTerm(it.key, contexts) }
            } else
                mergeAttributeInstancesResultsOnAttributeName(attributeAndResultsMap)
                    .mapKeys { JsonLdUtils.compactTerm(it.key, contexts) }
                    .mapValues {
                        it.value.map {
                            it as FullAttributeInstanceResult
                            JsonUtils.deserializeObject(it.payload)
                        }
                    }
        }

    /*
        Creates the simplified representation for each temporal entity attribute in the input Map
        The simplified representation is created from the attribute instance results of the temporal entity attribute
        It returns a Map with the same keys of the input Map and values corresponding to simplified representation
        of the temporal entity attribute.
    */
    private fun buildAttributesSimplifiedRepresentation(
        attributeAndResultsMap: Map<TemporalEntityAttribute, List<AttributeInstanceResult>>
    ): Map<TemporalEntityAttribute, SimplifiedTemporalAttribute> {
        return attributeAndResultsMap.mapValues {
            val attributeInstance = mutableMapOf<String, Any>(
                "type" to "Property"
            )
            it.key.datasetId?.let { attributeInstance["datasetId"] = it }
            attributeInstance["values"] = it.value.map {
                it as SimplifiedAttributeInstanceResult
                listOf(it.value, it.observedAt)
            }
            attributeInstance.toMap()
        }
    }

    /*
        Group the attribute instances results by temporal entity attribute name and return a Map with:
            - Key: temporal entity attribute name
            - Value: list of attribute instances results of the temporal entity attribute name
    */
    private fun mergeAttributeInstancesResultsOnAttributeName(
        attributeAndResultsMap: Map<TemporalEntityAttribute, List<AttributeInstanceResult>>
    ):
        Map<String, MutableList<AttributeInstanceResult>> {
            val resultMap: MutableMap<String, MutableList<AttributeInstanceResult>> = mutableMapOf()

            attributeAndResultsMap.forEach {
                if (resultMap.containsKey(it.key.attributeName))
                    resultMap[it.key.attributeName]!!.addAll(it.value)
                else
                    resultMap[it.key.attributeName] = it.value.toMutableList()
            }

            return resultMap.toMap()
        }

    /*
        Group the simplified representations by temporal entity attribute name and return a Map with:
            - Key: temporal entity attribute name
            - Value: list of the simplified temporal attributes of the temporal entity attribute name
    */
    private fun mergeSimplifiedTemporalAttributesOnAttributeName(
        attributeAndResultsMap: Map<TemporalEntityAttribute, SimplifiedTemporalAttribute>
    ):
        Map<String, MutableList<SimplifiedTemporalAttribute>> {
            val resultMap: MutableMap<String, MutableList<SimplifiedTemporalAttribute>> = mutableMapOf()

            attributeAndResultsMap.forEach {
                if (resultMap.containsKey(it.key.attributeName))
                    resultMap[it.key.attributeName]!!.add(it.value)
                else
                    resultMap[it.key.attributeName] = mutableListOf(it.value)
            }

            return resultMap.toMap()
        }
}
