package com.egm.stellio.search.scope

import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_LIST
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PREFIX
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUES
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedTemporalValue
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue

object TemporalScopeBuilder {

    fun buildScopeAttributeInstances(
        scopeInstances: List<ScopeInstanceResult>,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): Map<String, Any> =
        // if no instances (no scope or no history in the range), add an empty map to ignore in the final result
        if (scopeInstances.isEmpty())
            emptyMap()
        else if (temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.AGGREGATED_VALUES)
            buildScopeAggregatedRepresentation(
                scopeInstances,
                temporalEntitiesQuery.temporalQuery.aggrMethods!!
            )
        else if (temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.TEMPORAL_VALUES)
            buildScopeSimplifiedRepresentation(scopeInstances)
        else
            buildScopeFullRepresentation(scopeInstances)

    private fun buildScopeAggregatedRepresentation(
        scopeHistory: List<ScopeInstanceResult>,
        aggrMethods: List<TemporalQuery.Aggregate>
    ): Map<String, Any> {
        val attributeInstance = mutableMapOf<String, Any>(
            JSONLD_TYPE to listOf(NGSILD_PROPERTY_TYPE.uri)
        )

        aggrMethods.forEach { aggregate ->
            val valuesForAggregate = scopeHistory
                .map { scopeInstanceResult ->
                    scopeInstanceResult as AggregatedScopeInstanceResult
                    scopeInstanceResult.values
                }
                .flatten()
                .filter { aggregateResult ->
                    aggregateResult.aggregate == aggregate
                }
            attributeInstance[NGSILD_PREFIX + aggregate.method] =
                buildExpandedTemporalValue(valuesForAggregate) { aggregateResult ->
                    listOf(
                        mapOf(JSONLD_VALUE to aggregateResult.value),
                        mapOf(JSONLD_VALUE to aggregateResult.startDateTime),
                        mapOf(JSONLD_VALUE to aggregateResult.endDateTime)
                    )
                }
        }

        return mapOf(NGSILD_SCOPE_PROPERTY to attributeInstance.toMap())
    }

    private fun buildScopeSimplifiedRepresentation(
        scopeHistory: List<ScopeInstanceResult>
    ): Map<String, Any> {
        val attributeInstance = mapOf(
            JSONLD_TYPE to listOf(NGSILD_PROPERTY_TYPE.uri),
            NGSILD_PROPERTY_VALUES to
                buildExpandedTemporalValue(scopeHistory) { scopeInstanceResult ->
                    scopeInstanceResult as SimplifiedScopeInstanceResult
                    listOf(
                        mapOf(
                            JSONLD_LIST to scopeInstanceResult.scopes.map { mapOf(JSONLD_VALUE to it) }
                        ),
                        mapOf(JSONLD_VALUE to scopeInstanceResult.time)
                    )
                }
        )

        return mapOf(NGSILD_SCOPE_PROPERTY to attributeInstance)
    }

    private fun buildScopeFullRepresentation(
        scopeHistory: List<ScopeInstanceResult>
    ): Map<String, Any> =
        mapOf(
            NGSILD_SCOPE_PROPERTY to scopeHistory.map { scopeInstanceResult ->
                scopeInstanceResult as FullScopeInstanceResult
                mapOf(
                    JSONLD_TYPE to listOf(NGSILD_PROPERTY_TYPE.uri),
                    NGSILD_PROPERTY_VALUE to scopeInstanceResult.scopes.map { mapOf(JSONLD_VALUE to it) },
                    NGSILD_PREFIX + scopeInstanceResult.timeproperty to
                        buildNonReifiedTemporalValue(scopeInstanceResult.time)
                )
            }
        )
}
