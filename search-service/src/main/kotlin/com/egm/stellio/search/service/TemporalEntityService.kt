package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.CompactedJsonLdAttribute
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.util.*
import org.springframework.stereotype.Service
import java.net.URI

@Service
class TemporalEntityService {

    fun buildTemporalEntity(
        entityId: URI,
        attributeAndResultsMap: Map<TemporalEntityAttribute, List<AttributeInstanceResult>>,
        contexts: List<String>,
        withTemporalValues: Boolean
    ): CompactedJsonLdEntity {
        val temporalAttributes = buildTemporalAttributes(
            attributeAndResultsMap.filterValues { it.isNotEmpty() },
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
        contexts: List<String>,
        withTemporalValues: Boolean
    ):
        Map<String, Any> {
            return if (withTemporalValues) {
                val attributes = buildAttributesSimplifiedRepresentation(attributeAndResultsMap)
                mergeAttributeInstancesOnAttributeName(attributes)
                    .mapKeys { JsonLdUtils.compactTerm(it.key, contexts) }
            } else
                mergeAttributeInstancesResultsOnAttributeName(attributeAndResultsMap)
                    .mapKeys { JsonLdUtils.compactTerm(it.key, contexts) }
                    .mapValues {
                        it.value.map { JsonUtils.deserializeObject(it.payload!!) }
                    }
        }

    private fun buildAttributesSimplifiedRepresentation(
        attributeAndResultsMap: Map<TemporalEntityAttribute, List<AttributeInstanceResult>>
    ): Map<TemporalEntityAttribute, CompactedJsonLdAttribute> {
        return attributeAndResultsMap.mapValues {
            val attributeInstance = mutableMapOf<String, Any>(
                "type" to "Property"
            )
            it.key.datasetId?.let { attributeInstance["datasetId"] = it }
            attributeInstance["values"] = it.value.map { listOf(it.value, it.observedAt) }
            attributeInstance.toMap()
        }
    }

    private fun mergeAttributeInstancesResultsOnAttributeName(
        attributeAndResultsMap: Map<TemporalEntityAttribute, List<AttributeInstanceResult>>
    ):
        MutableMap<String, MutableList<AttributeInstanceResult>> {
            val resultMap: MutableMap<String, MutableList<AttributeInstanceResult>> = mutableMapOf()

            attributeAndResultsMap.forEach {
                if (resultMap.containsKey(it.key.attributeName))
                    resultMap[it.key.attributeName]!!.addAll(it.value)
                else
                    resultMap[it.key.attributeName] = it.value.toMutableList()
            }

            return resultMap
        }

    private fun mergeAttributeInstancesOnAttributeName(
        attributeAndResultsMap: Map<TemporalEntityAttribute, CompactedJsonLdAttribute>
    ):
        MutableMap<String, MutableList<CompactedJsonLdAttribute>> {
            val resultMap: MutableMap<String, MutableList<CompactedJsonLdAttribute>> = mutableMapOf()

            attributeAndResultsMap.forEach {
                if (resultMap.containsKey(it.key.attributeName))
                    resultMap[it.key.attributeName]!!.add(it.value)
                else
                    resultMap[it.key.attributeName] = mutableListOf(it.value)
            }

            return resultMap
        }
}
