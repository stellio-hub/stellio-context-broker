package com.egm.stellio.search.temporal.util

import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.scope.TemporalScopeBuilder
import com.egm.stellio.search.temporal.model.AggregatedAttributeInstanceResult
import com.egm.stellio.search.temporal.model.AttributeInstanceResult
import com.egm.stellio.search.temporal.model.EntityTemporalResult
import com.egm.stellio.search.temporal.model.FullAttributeInstanceResult
import com.egm.stellio.search.temporal.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JSONLD_JSON_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_IRI
import com.egm.stellio.shared.model.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.model.NGSILD_JSONPROPERTY_JSON
import com.egm.stellio.shared.model.NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP
import com.egm.stellio.shared.model.NGSILD_PREFIX
import com.egm.stellio.shared.model.NGSILD_VALUE_TERM
import com.egm.stellio.shared.model.NGSILD_VOCABPROPERTY_VOCAB
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SUB
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedTemporalValue
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue
import com.egm.stellio.shared.util.JsonLdUtils.expandGeoPropertyFragment
import com.egm.stellio.shared.util.JsonUtils.deserializeListOfObjects
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.wktToGeoJson

typealias SimplifiedTemporalAttribute = Map<String, Any>
typealias AttributesWithInstances = Map<Attribute, List<AttributeInstanceResult>>

object TemporalEntityBuilder {

    fun buildTemporalEntities(
        queryResult: List<EntityTemporalResult>,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        coreContext: String
    ): List<ExpandedEntity> =
        queryResult.map {
            buildTemporalEntity(it, temporalEntitiesQuery, coreContext)
        }

    fun buildTemporalEntity(
        entityTemporalResult: EntityTemporalResult,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        coreContext: String
    ): ExpandedEntity {
        val temporalAttributes = buildTemporalAttributes(
            entityTemporalResult.attributesWithInstances,
            temporalEntitiesQuery,
            coreContext
        )

        val scopeAttributeInstances = TemporalScopeBuilder.buildScopeAttributeInstances(
            entityTemporalResult.scopeHistory,
            temporalEntitiesQuery
        )

        val expandedTemporalEntity = entityTemporalResult.entity.serializeProperties()
            .plus(temporalAttributes)
            .plus(scopeAttributeInstances)
        return ExpandedEntity(expandedTemporalEntity)
            .filterPickAndOmit(temporalEntitiesQuery.entitiesQuery.pick, temporalEntitiesQuery.entitiesQuery.omit)
    }

    private fun buildTemporalAttributes(
        attributeAndResultsMap: AttributesWithInstances,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        coreContext: String
    ): Map<String, Any> =
        if (temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.TEMPORAL_VALUES) {
            val attributes = buildAttributesSimplifiedRepresentation(attributeAndResultsMap, coreContext)
            mergeSimplifiedTemporalAttributesOnAttributeName(attributes)
        } else if (temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.AGGREGATED_VALUES) {
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
        if (instancePayload[JSONLD_TYPE_KW] == NGSILD_GEOPROPERTY_TYPE.uri)
            instancePayload.plus(JSONLD_VALUE_KW to wktToGeoJson(instancePayload[NGSILD_VALUE_TERM]!! as String))
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
        attributeAndResultsMap: AttributesWithInstances,
        coreContext: String
    ): Map<Attribute, SimplifiedTemporalAttribute> =
        attributeAndResultsMap.mapValues {
            val attributeInstance = mutableMapOf<String, Any>(
                JSONLD_TYPE_KW to listOf(it.key.attributeType.toExpandedName())
            )
            it.key.datasetId?.let { datasetId ->
                attributeInstance[NGSILD_DATASET_ID_IRI] = buildNonReifiedPropertyValue(datasetId.toString())
            }
            val valuesKey = it.key.attributeType.toSimplifiedRepresentationKey()
            attributeInstance[valuesKey] =
                buildExpandedTemporalValue(it.value) { attributeInstanceResult ->
                    attributeInstanceResult as SimplifiedAttributeInstanceResult
                    when (it.key.attributeType) {
                        Attribute.AttributeType.JsonProperty -> {
                            // flaky way to know if the serialized value is a JSON object or an array of JSON objects
                            val deserializedJsonValue: Any =
                                if ((attributeInstanceResult.value as String).startsWith("["))
                                    deserializeListOfObjects(attributeInstanceResult.value)
                                else deserializeObject(attributeInstanceResult.value)
                            listOf(
                                mapOf(
                                    NGSILD_JSONPROPERTY_JSON to listOf(
                                        mapOf(
                                            JSONLD_TYPE_KW to JSONLD_JSON_KW,
                                            JSONLD_VALUE_KW to deserializedJsonValue
                                        )
                                    )
                                ),
                                mapOf(JSONLD_VALUE_KW to attributeInstanceResult.time)
                            )
                        }
                        Attribute.AttributeType.LanguageProperty -> {
                            listOf(
                                mapOf(
                                    NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP to
                                        deserializeListOfObjects(attributeInstanceResult.value as String)
                                ),
                                mapOf(JSONLD_VALUE_KW to attributeInstanceResult.time)
                            )
                        }
                        Attribute.AttributeType.VocabProperty -> {
                            listOf(
                                mapOf(
                                    NGSILD_VOCABPROPERTY_VOCAB to
                                        deserializeListOfObjects(attributeInstanceResult.value as String)
                                ),
                                mapOf(JSONLD_VALUE_KW to attributeInstanceResult.time)
                            )
                        }
                        Attribute.AttributeType.GeoProperty -> {
                            val expendedGeoProperty = expandGeoPropertyFragment(
                                attributeInstanceResult.value as Map<String, Any>,
                                listOf(coreContext)
                            )
                            listOf(expendedGeoProperty)
                        }
                        else -> {
                            listOf(
                                mapOf(JSONLD_VALUE_KW to attributeInstanceResult.value),
                                mapOf(JSONLD_VALUE_KW to attributeInstanceResult.time)
                            )
                        }
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
        attributeAndResultsMap: AttributesWithInstances,
        aggrMethods: List<TemporalQuery.Aggregate>
    ): Map<Attribute, SimplifiedTemporalAttribute> {
        return attributeAndResultsMap.mapValues {
            val attributeInstance = mutableMapOf<String, Any>(
                JSONLD_TYPE_KW to listOf(it.key.attributeType.toExpandedName())
            )
            it.key.datasetId?.let { datasetId ->
                attributeInstance[NGSILD_DATASET_ID_IRI] = buildNonReifiedPropertyValue(datasetId.toString())
            }

            val aggregatedResultsForAttributes = it.value
                .map { attributeInstanceResult ->
                    attributeInstanceResult as AggregatedAttributeInstanceResult
                    attributeInstanceResult.values
                }
                .flatten()

            aggrMethods.forEach { aggregate ->
                val resultsForAggregate = aggregatedResultsForAttributes.filter { aggregateResult ->
                    aggregateResult.aggregate.method == aggregate.method
                }
                attributeInstance[NGSILD_PREFIX + aggregate.method] =
                    buildExpandedTemporalValue(resultsForAggregate) { aggregateResult ->
                        listOf(
                            mapOf(JSONLD_VALUE_KW to aggregateResult.value),
                            mapOf(JSONLD_VALUE_KW to aggregateResult.startDateTime),
                            mapOf(JSONLD_VALUE_KW to aggregateResult.endDateTime)
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
        attributeAndResultsMap: AttributesWithInstances
    ): Map<ExpandedTerm, List<FullAttributeInstanceResult>> =
        attributeAndResultsMap.toList()
            .groupBy { (attribute, _) ->
                attribute.attributeName
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
        attributeAndResultsMap: Map<Attribute, SimplifiedTemporalAttribute>
    ): Map<ExpandedTerm, List<SimplifiedTemporalAttribute>> =
        attributeAndResultsMap.toList()
            .groupBy { (attribute, _) ->
                attribute.attributeName
            }
            .toMap()
            .mapValues {
                it.value.map { (_, simplifiedTemporalAttribute) ->
                    simplifiedTemporalAttribute
                }
            }

    /**
     * After some discussion (see https://github.com/stellio-hub/stellio-context-broker/issues/1143),
     * it has been decided that, since there are other cases where single objects are forced to be wrapped as arrays,
     * such temporal instances should always be returned as arrays
     * (even if there is only one element and if it is kind of overwriting of normal JSON-LD compaction).
     */
    fun CompactedEntity.wrapSingleValuesToList(temporalRepresentation: TemporalRepresentation): CompactedEntity =
        if (temporalRepresentation == TemporalRepresentation.NORMALIZED) {
            this.mapValues { (_, value) ->
                if (value is Map<*, *>) listOf(value)
                else value
            }
        } else this

    fun List<CompactedEntity>.wrapSingleValuesToList(
        temporalRepresentation: TemporalRepresentation
    ): List<CompactedEntity> =
        this.map { it.wrapSingleValuesToList(temporalRepresentation) }
}
