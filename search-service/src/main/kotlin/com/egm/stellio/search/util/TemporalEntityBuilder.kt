package com.egm.stellio.search.util

import com.egm.stellio.search.model.*
import com.egm.stellio.search.scope.TemporalScopeBuilder
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SUB
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_JSON
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_JSONPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LANGUAGEPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PREFIX
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedTemporalValue
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue
import com.egm.stellio.shared.util.JsonUtils.deserializeListOfObjects
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.wktToGeoJson

typealias SimplifiedTemporalAttribute = Map<String, Any>
typealias TemporalEntityAttributeInstancesResult = Map<TemporalEntityAttribute, List<AttributeInstanceResult>>

object TemporalEntityBuilder {

    fun buildTemporalEntities(
        queryResult: List<EntityTemporalResult>,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        contexts: List<String>
    ): List<ExpandedEntity> =
        queryResult.map {
            buildTemporalEntity(it, temporalEntitiesQuery, contexts)
        }

    fun buildTemporalEntity(
        entityTemporalResult: EntityTemporalResult,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        contexts: List<String>
    ): ExpandedEntity {
        val temporalAttributes = buildTemporalAttributes(
            entityTemporalResult.teaInstancesResult,
            temporalEntitiesQuery
        )

        val scopeAttributeInstances = TemporalScopeBuilder.buildScopeAttributeInstances(
            entityTemporalResult.entityPayload,
            entityTemporalResult.scopeHistory,
            temporalEntitiesQuery
        )

        val expandedTemporalEntity = entityTemporalResult.entityPayload.serializeProperties()
            .plus(temporalAttributes)
            .plus(scopeAttributeInstances)
        return ExpandedEntity(expandedTemporalEntity, contexts)
    }

    private fun buildTemporalAttributes(
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult,
        temporalEntitiesQuery: TemporalEntitiesQuery,
    ): Map<String, Any> =
        if (temporalEntitiesQuery.withTemporalValues) {
            val attributes = buildAttributesSimplifiedRepresentation(attributeAndResultsMap)
            mergeSimplifiedTemporalAttributesOnAttributeName(attributes)
        } else if (temporalEntitiesQuery.withAggregatedValues) {
            val attributes = buildAttributesAggregatedRepresentation(
                attributeAndResultsMap,
                temporalEntitiesQuery.temporalQuery.aggrMethods!!
            )
            mergeSimplifiedTemporalAttributesOnAttributeName(attributes)
        } else {
            mergeFullTemporalAttributesOnAttributeName(attributeAndResultsMap)
                .mapValues { (_, attributeInstanceResults) ->
                    attributeInstanceResults.map { attributeInstanceResult ->
                        deserializeObject(attributeInstanceResult.payload)
                            .let { instancePayload ->
                                injectSub(temporalEntitiesQuery, attributeInstanceResult, instancePayload)
                            }.let { instancePayload ->
                                convertGeoProperty(instancePayload)
                            }.plus(
                                Pair(
                                    NGSILD_PREFIX + attributeInstanceResult.timeproperty,
                                    buildNonReifiedTemporalValue(attributeInstanceResult.time)
                                )
                            )
                    }
                }
        }

    private fun convertGeoProperty(instancePayload: Map<String, Any>): Map<String, Any> =
        if (instancePayload[JSONLD_TYPE] == NGSILD_GEOPROPERTY_TYPE.uri)
            instancePayload.plus(JSONLD_VALUE to wktToGeoJson(instancePayload[JSONLD_VALUE_TERM]!! as String))
        else instancePayload

    private fun injectSub(
        temporalEntitiesQuery: TemporalEntitiesQuery,
        attributeInstanceResult: FullAttributeInstanceResult,
        instancePayload: Map<String, Any>
    ): Map<String, Any> =
        if (temporalEntitiesQuery.withAudit && attributeInstanceResult.sub != null)
            instancePayload.plus(Pair(AUTH_PROP_SUB, buildExpandedPropertyValue(attributeInstanceResult.sub)))
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
    ): Map<TemporalEntityAttribute, SimplifiedTemporalAttribute> =
        attributeAndResultsMap.mapValues {
            val attributeInstance = mutableMapOf<String, Any>(
                JSONLD_TYPE to listOf(it.key.attributeType.toExpandedName())
            )
            it.key.datasetId?.let { datasetId ->
                attributeInstance[NGSILD_DATASET_ID_PROPERTY] = buildNonReifiedPropertyValue(datasetId.toString())
            }
            val valuesKey = it.key.attributeType.toSimpliedRepresentationKey()
            attributeInstance[valuesKey] =
                buildExpandedTemporalValue(it.value) { attributeInstanceResult ->
                    attributeInstanceResult as SimplifiedAttributeInstanceResult
                    if (it.key.attributeType == TemporalEntityAttribute.AttributeType.JsonProperty) {
                        // flaky way to know if the serialized value is a JSON object or an array of JSON objects
                        val deserializedJsonValue: Any =
                            if ((attributeInstanceResult.value as String).startsWith("["))
                                deserializeListOfObjects(attributeInstanceResult.value)
                            else deserializeObject(attributeInstanceResult.value)
                        listOf(
                            mapOf(
                                NGSILD_JSONPROPERTY_VALUE to listOf(
                                    mapOf(
                                        JSONLD_TYPE to JSONLD_JSON,
                                        JSONLD_VALUE to deserializedJsonValue
                                    )
                                )
                            ),
                            mapOf(JSONLD_VALUE to attributeInstanceResult.time)
                        )
                    } else if (it.key.attributeType == TemporalEntityAttribute.AttributeType.LanguageProperty) {
                        listOf(
                            mapOf(
                                NGSILD_LANGUAGEPROPERTY_VALUE to
                                    deserializeListOfObjects(attributeInstanceResult.value as String)
                            ),
                            mapOf(JSONLD_VALUE to attributeInstanceResult.time)
                        )
                    } else {
                        listOf(
                            mapOf(JSONLD_VALUE to attributeInstanceResult.value),
                            mapOf(JSONLD_VALUE to attributeInstanceResult.time)
                        )
                    }
                }
            attributeInstance.toMap()
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
                JSONLD_TYPE to listOf(it.key.attributeType.toExpandedName())
            )
            it.key.datasetId?.let { datasetId ->
                attributeInstance[NGSILD_DATASET_ID_PROPERTY] = buildNonReifiedPropertyValue(datasetId.toString())
            }

            val aggregatedResultsForTEA = it.value
                .map { attributeInstanceResult ->
                    attributeInstanceResult as AggregatedAttributeInstanceResult
                    attributeInstanceResult.values
                }
                .flatten()

            aggrMethods.forEach { aggregate ->
                val resultsForAggregate = aggregatedResultsForTEA.filter { aggregateResult ->
                    aggregateResult.aggregate.method == aggregate.method
                }
                attributeInstance[NGSILD_PREFIX + aggregate.method] =
                    buildExpandedTemporalValue(resultsForAggregate) { aggregateResult ->
                        listOf(
                            mapOf(JSONLD_VALUE to aggregateResult.value),
                            mapOf(JSONLD_VALUE to aggregateResult.startDateTime),
                            mapOf(JSONLD_VALUE to aggregateResult.endDateTime)
                        )
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
