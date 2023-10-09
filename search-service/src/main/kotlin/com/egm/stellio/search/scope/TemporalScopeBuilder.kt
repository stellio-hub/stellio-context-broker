package com.egm.stellio.search.scope

import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SCOPE_TERM

object TemporalScopeBuilder {

    fun buildScopeAttributeInstances(
        entityPayload: EntityPayload,
        scopeInstances: List<ScopeInstanceResult>,
        temporalEntitiesQuery: TemporalEntitiesQuery
    ): Map<String, Any> =
        // if no history, add an empty map only if entity has a scope
        if (entityPayload.scopes == null && scopeInstances.isEmpty())
            emptyMap()
        // if no history but entity has a scope, add an empty scope list (no history in the given time range)
        else if (scopeInstances.isEmpty())
            mapOf(NGSILD_SCOPE_TERM to emptyList<String>())
        else if (temporalEntitiesQuery.withAggregatedValues)
            buildScopeAggregatedRepresentation(
                scopeInstances,
                temporalEntitiesQuery.temporalQuery.aggrMethods!!
            )
        else if (temporalEntitiesQuery.withTemporalValues)
            mapOf(
                NGSILD_SCOPE_TERM to mapOf(
                    JSONLD_TYPE_TERM to "Property",
                    "values" to scopeInstances.map {
                        it as SimplifiedScopeInstanceResult
                        listOf(it.scopes, it.time)
                    }
                )
            )
        else
            mapOf(
                NGSILD_SCOPE_TERM to scopeInstances.map {
                    it as FullScopeInstanceResult
                    mapOf(
                        JSONLD_TYPE_TERM to "Property",
                        JSONLD_VALUE_TERM to it.scopes,
                        it.timeproperty to it.time
                    )
                }
            )

    private fun buildScopeAggregatedRepresentation(
        scopeHistory: List<ScopeInstanceResult>,
        aggrMethods: List<TemporalQuery.Aggregate>
    ): Map<String, Any> {
        val attributeInstance = mutableMapOf<String, Any>(
            JSONLD_TYPE_TERM to "Property"
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
            attributeInstance[aggregate.method] = valuesForAggregate.map { aggregateResult ->
                listOf(aggregateResult.value, aggregateResult.startDateTime, aggregateResult.endDateTime)
            }
        }

        return mapOf(NGSILD_SCOPE_TERM to attributeInstance.toMap())
    }
}
