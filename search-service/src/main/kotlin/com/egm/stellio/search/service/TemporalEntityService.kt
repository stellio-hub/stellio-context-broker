package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SUB
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils
import org.springframework.stereotype.Service

typealias SimplifiedTemporalAttribute = Map<String, Any>
typealias TemporalEntityAttributeInstancesResult = Map<TemporalEntityAttribute, List<AttributeInstanceResult>>

@Service
class TemporalEntityService {

    fun buildTemporalEntities(
        queryResult: List<Pair<EntityPayload, Map<TemporalEntityAttribute, List<AttributeInstanceResult>>>>,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        contexts: List<String>
    ): List<CompactedJsonLdEntity> {
        return queryResult.map {
            buildTemporalEntity(it.first, it.second, temporalEntitiesQuery, contexts)
        }
    }

    fun buildTemporalEntity(
        entityPayload: EntityPayload,
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        contexts: List<String>
    ): CompactedJsonLdEntity {
        val temporalAttributes = buildTemporalAttributes(
            attributeAndResultsMap,
            temporalEntitiesQuery,
            contexts
        )
        return entityPayload.serializeProperties(
            withSysAttrs = temporalEntitiesQuery.queryParams.includeSysAttrs,
            withCompactTerms = true,
            contexts
        ).plus(temporalAttributes)
    }

    private fun buildTemporalAttributes(
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        contexts: List<String>
    ): Map<String, Any> {
        return if (
            temporalEntitiesQuery.withTemporalValues ||
            temporalEntitiesQuery.temporalQuery.timeBucket != null
        ) {
            val attributes = buildAttributesSimplifiedRepresentation(attributeAndResultsMap)
            mergeSimplifiedTemporalAttributesOnAttributeName(attributes)
                .mapKeys { JsonLdUtils.compactTerm(it.key, contexts) }
                .mapValues {
                    if (it.value.size == 1) it.value.first()
                    else it.value
                }
        } else {
            mergeFullTemporalAttributesOnAttributeName(attributeAndResultsMap)
                .mapKeys { JsonLdUtils.compactTerm(it.key, contexts) }
                .mapValues {
                    it.value.map { attributeInstanceResult ->
                        attributeInstanceResult as FullAttributeInstanceResult
                        JsonUtils.deserializeObject(attributeInstanceResult.payload)
                            .let { jsonMap ->
                                if (temporalEntitiesQuery.withAudit && attributeInstanceResult.sub != null)
                                    jsonMap.plus(Pair(AUTH_TERM_SUB, attributeInstanceResult.sub))
                                else jsonMap
                            }
                    }
                }
        }
    }

    /**
     * Creates the simplified representation for each temporal entity attribute in the input map.
     *
     * The simplified representation is created from the attribute instance results of the temporal entity attribute.
     *
     * It returns a Map with the same keys as the input map and values corresponding to simplified representation
     * of the temporal entity attribute.
     */
    private fun buildAttributesSimplifiedRepresentation(
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult
    ): Map<TemporalEntityAttribute, SimplifiedTemporalAttribute> {
        return attributeAndResultsMap.mapValues {
            val attributeInstance = mutableMapOf<String, Any>(
                "type" to it.key.attributeType.toString()
            )
            val valuesKey =
                if (it.key.attributeType == TemporalEntityAttribute.AttributeType.Property)
                    "values"
                else
                    "objects"
            it.key.datasetId?.let { attributeInstance["datasetId"] = it }
            attributeInstance[valuesKey] = it.value.map { attributeInstanceResult ->
                attributeInstanceResult as SimplifiedAttributeInstanceResult
                listOf(attributeInstanceResult.value, attributeInstanceResult.time)
            }
            attributeInstance.toMap()
        }
    }

    /**
     * Group the attribute instances results by temporal entity attribute name and return a map with:
     * - Key: (expanded) name of the temporal entity attribute
     * - Value: list of the full representation of the attribute instances
     */
    private fun mergeFullTemporalAttributesOnAttributeName(
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult
    ): Map<String, List<AttributeInstanceResult>> =
        attributeAndResultsMap.toList()
            .groupBy { (temporalEntityAttribute, _) ->
                temporalEntityAttribute.attributeName
            }
            .toMap()
            .mapValues {
                it.value.map { (_, attributeInstancesResults) ->
                    attributeInstancesResults
                }.flatten()
            }

    /**
     * Group the simplified representations by temporal entity attribute name and return a map with:
     * - Key: (expanded) name of the temporal entity attribute
     * - Value: list of the simplified temporal representation of the attribute instances
     */
    private fun mergeSimplifiedTemporalAttributesOnAttributeName(
        attributeAndResultsMap: Map<TemporalEntityAttribute, SimplifiedTemporalAttribute>
    ): Map<String, List<SimplifiedTemporalAttribute>> =
        attributeAndResultsMap.toList()
            .groupBy { (temporalEntityAttribute, _) ->
                temporalEntityAttribute.attributeName
            }
            .toMap()
            .mapValues {
                it.value.map { (_, simplifiedTemporalAttribute) ->
                    simplifiedTemporalAttribute
                }
            }
}
