package com.egm.stellio.search.util

import com.egm.stellio.search.model.*
import com.egm.stellio.search.scope.TemporalScopeBuilder
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SUB
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.compactFragment

typealias SimplifiedTemporalAttribute = Map<String, Any>
typealias TemporalEntityAttributeInstancesResult = Map<TemporalEntityAttribute, List<AttributeInstanceResult>>

object TemporalEntityBuilder {

    fun buildTemporalEntities(
        queryResult: List<EntityTemporalResult>,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        contexts: List<String>
    ): List<CompactedJsonLdEntity> =
        queryResult.map {
            buildTemporalEntity(it, temporalEntitiesQuery, contexts)
        }

    fun buildTemporalEntity(
        entityTemporalResult: EntityTemporalResult,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        contexts: List<String>
    ): CompactedJsonLdEntity {
        val temporalAttributes = buildTemporalAttributes(
            entityTemporalResult.teaInstancesResult,
            temporalEntitiesQuery,
            contexts
        )

        val scopeAttributeInstances = TemporalScopeBuilder.buildScopeAttributeInstances(
            entityTemporalResult.entityPayload,
            entityTemporalResult.scopeHistory,
            temporalEntitiesQuery
        )

        return entityTemporalResult.entityPayload.serializeProperties(
            withSysAttrs = temporalEntitiesQuery.queryParams.includeSysAttrs,
            withCompactTerms = true,
            contexts
        ).plus(temporalAttributes)
            .plus(scopeAttributeInstances)
    }

    private fun buildTemporalAttributes(
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        contexts: List<String>
    ): Map<String, Any> =
        if (temporalEntitiesQuery.withTemporalValues) {
            val attributes = buildAttributesSimplifiedRepresentation(attributeAndResultsMap)
            mergeSimplifiedTemporalAttributesOnAttributeName(attributes)
                .mapKeys { JsonLdUtils.compactTerm(it.key, contexts) }
                .mapValues {
                    if (it.value.size == 1) it.value.first()
                    else it.value
                }
        } else if (temporalEntitiesQuery.withAggregatedValues) {
            val attributes = buildAttributesAggregatedRepresentation(
                attributeAndResultsMap,
                temporalEntitiesQuery.temporalQuery.aggrMethods!!
            )
            mergeSimplifiedTemporalAttributesOnAttributeName(attributes)
                .mapKeys { JsonLdUtils.compactTerm(it.key, contexts) }
                .mapValues {
                    if (it.value.size == 1) it.value.first()
                    else it.value
                }
        } else {
            mergeFullTemporalAttributesOnAttributeName(attributeAndResultsMap)
                .mapKeys { JsonLdUtils.compactTerm(it.key, contexts) }
                .mapValues { (_, attributeInstanceResults) ->
                    attributeInstanceResults.map { attributeInstanceResult ->
                        JsonUtils.deserializeObject(attributeInstanceResult.payload)
                            .let { instancePayload ->
                                injectSub(temporalEntitiesQuery, attributeInstanceResult, instancePayload)
                            }.let { instancePayload ->
                                compactAttributeInstance(instancePayload, contexts)
                            }.let { instancePayload ->
                                convertGeoProperty(instancePayload)
                            }.plus(
                                attributeInstanceResult.timeproperty to attributeInstanceResult.time.toNgsiLdFormat()
                            )
                    }
                }
        }

    // FIXME in the history of attributes, we have a mix of
    //   - compacted fragments (before v2)
    //   - expanded fragments (since v2)
    //  to avoid un-necessary (expensive) compactions, quick check to see if the fragment
    //  is already compacted
    private fun compactAttributeInstance(instancePayload: Map<String, Any>, contexts: List<String>): Map<String, Any> =
        if (instancePayload.containsKey(JSONLD_TYPE))
            compactFragment(instancePayload, contexts).minus(JSONLD_CONTEXT)
        else instancePayload

    private fun convertGeoProperty(instancePayload: Map<String, Any>): Map<String, Any> =
        if (instancePayload[JSONLD_TYPE_TERM] == "GeoProperty")
            instancePayload.plus(JSONLD_VALUE_TERM to wktToGeoJson(instancePayload[JSONLD_VALUE_TERM]!! as String))
        else instancePayload

    private fun injectSub(
        temporalEntitiesQuery: TemporalEntitiesQuery,
        attributeInstanceResult: FullAttributeInstanceResult,
        instancePayload: Map<String, Any>
    ): Map<String, Any> =
        if (temporalEntitiesQuery.withAudit && attributeInstanceResult.sub != null)
            instancePayload.plus(Pair(AUTH_TERM_SUB, attributeInstanceResult.sub))
        else instancePayload

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
                JSONLD_TYPE_TERM to it.key.attributeType.toString()
            )
            it.key.datasetId?.let { datasetId -> attributeInstance["datasetId"] = datasetId }
            val valuesKey =
                when (it.key.attributeType) {
                    TemporalEntityAttribute.AttributeType.Property -> "values"
                    TemporalEntityAttribute.AttributeType.Relationship -> "objects"
                    TemporalEntityAttribute.AttributeType.GeoProperty -> "values"
                }
            attributeInstance[valuesKey] = it.value.map { attributeInstanceResult ->
                attributeInstanceResult as SimplifiedAttributeInstanceResult
                listOf(attributeInstanceResult.value, attributeInstanceResult.time)
            }
            attributeInstance.toMap()
        }
    }

    /**
     * Creates the aggregated representation for each temporal entity attribute in the input map.
     *
     * The aggregated representation is created from the attribute instance results of the temporal entity attribute.
     *
     * It returns a map with the same keys as the input map and values corresponding to aggregated representations
     * as described in 4.5.19.0
     */
    private fun buildAttributesAggregatedRepresentation(
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult,
        aggrMethods: List<TemporalQuery.Aggregate>
    ): Map<TemporalEntityAttribute, SimplifiedTemporalAttribute> {
        return attributeAndResultsMap.mapValues {
            val attributeInstance = mutableMapOf<String, Any>(
                JSONLD_TYPE_TERM to it.key.attributeType.toString()
            )
            it.key.datasetId?.let { datasetId -> attributeInstance["datasetId"] = datasetId }

            aggrMethods.forEach { aggregate ->
                val valuesForAggregate = it.value
                    .map { attributeInstanceResult ->
                        attributeInstanceResult as AggregatedAttributeInstanceResult
                        attributeInstanceResult.values
                    }
                    .flatten()
                    .filter { aggregateResult ->
                        aggregateResult.aggregate == aggregate
                    }
                attributeInstance[aggregate.method] = valuesForAggregate.map { aggregateResult ->
                    listOf(aggregateResult.value, aggregateResult.startDateTime, aggregateResult.endDateTime)
                }
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
    ): Map<ExpandedTerm, List<FullAttributeInstanceResult>> =
        attributeAndResultsMap.toList()
            .groupBy { (temporalEntityAttribute, _) ->
                temporalEntityAttribute.attributeName
            }
            .toMap()
            .mapValues {
                it.value.map { (_, attributeInstancesResults) ->
                    attributeInstancesResults as List<FullAttributeInstanceResult>
                }.flatten()
            }

    /**
     * Group the simplified representations by temporal entity attribute name and return a map with:
     * - Key: (expanded) name of the temporal entity attribute
     * - Value: list of the simplified temporal representation of the attribute instances
     */
    private fun mergeSimplifiedTemporalAttributesOnAttributeName(
        attributeAndResultsMap: Map<TemporalEntityAttribute, SimplifiedTemporalAttribute>
    ): Map<ExpandedTerm, List<SimplifiedTemporalAttribute>> =
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
