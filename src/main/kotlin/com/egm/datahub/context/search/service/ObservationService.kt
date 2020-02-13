package com.egm.datahub.context.search.service

import com.egm.datahub.context.search.model.*
import com.egm.datahub.context.search.util.NgsiLdParsingUtils
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.query.Criteria.where
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class ObservationService(
    private val databaseClient: DatabaseClient,
    private val contextRegistryService: ContextRegistryService
) {

    fun create(observation: Observation): Mono<Int> {
        return databaseClient.insert()
            .into(Observation::class.java)
            .using(observation)
            .fetch()
            .rowsUpdated()
    }

    fun search(temporalQuery: TemporalQuery, bearerToken: String): Mono<Pair<Map<String, Any>, List<String>>> {

        val fromSelectSpec = databaseClient
            .select()
            .from("observation")
            .project("value", "observed_at", "attribute_name")

        val timeCriteriaStep = when (temporalQuery.timerel) {
            TemporalQuery.Timerel.BEFORE -> where("observed_at").lessThan(temporalQuery.time)
            TemporalQuery.Timerel.AFTER -> where("observed_at").greaterThan(temporalQuery.time)
            else -> where("observed_at").greaterThan(temporalQuery.time)
                .and("observed_at").lessThan(temporalQuery.endTime!!)
        }

        // TODO we actually only support queries providing an entity id
        val results = if (temporalQuery.entityId != null)
            fromSelectSpec.matching(timeCriteriaStep.and("observed_by").`is`(temporalQuery.entityId)).fetch().all()
        else
            fromSelectSpec.matching(timeCriteriaStep).fetch().all()

        return results.collectList()
            .zipWith(contextRegistryService.getEntityById(temporalQuery.entityId!!, bearerToken))
            .map {
                val entity = it.t2.first.toMutableMap()

                // attribute_name is the name of the temporal property we want to update
                val attributeName = it.t1.first()["attribute_name"]!!
                val expandedAttributeName = NgsiLdParsingUtils.expandJsonLdKey(attributeName as String, it.t2.second)

                // extract the temporal property from the raw entity and remove the value property from it
                val propertyToEnrich = NgsiLdParsingUtils.expandValueAsMap(entity[expandedAttributeName]!!).toMutableMap()
                propertyToEnrich.remove(NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE)

                // insert the values property with data retrieved from DB
                val valuesKey = NgsiLdParsingUtils.expandJsonLdKey("values", it.t2.second)
                val simplifiedValues = it.t1.map { listOf(it["VALUE"], it["OBSERVED_AT"]) }
                propertyToEnrich[valuesKey!!] = simplifiedValues

                // and finally update the raw entity with the updated temporal property
                entity.remove(expandedAttributeName)
                entity[expandedAttributeName!!] = listOf(propertyToEnrich)

                Pair(entity, it.t2.second)
            }
    }
}
