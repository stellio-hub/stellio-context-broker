package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.query.Criteria.where
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class ObservationService(
    private val databaseClient: DatabaseClient
) {

    fun create(observation: Observation): Mono<Int> {
        return databaseClient.insert()
            .into(Observation::class.java)
            .using(observation)
            .fetch()
            .rowsUpdated()
    }

    fun search(temporalQuery: TemporalQuery, entityTemporalProperty: EntityTemporalProperty): Mono<List<Map<String, Any>>> {

        val fromSelectSpec = databaseClient
            .select()
            .from("observation")
            .project("value", "observed_at", "attribute_name")

        var criteriaStep = when (temporalQuery.timerel) {
            TemporalQuery.Timerel.BEFORE -> where("observed_at").lessThan(temporalQuery.time)
            TemporalQuery.Timerel.AFTER -> where("observed_at").greaterThan(temporalQuery.time)
            else -> where("observed_at").greaterThan(temporalQuery.time)
                .and("observed_at").lessThan(temporalQuery.endTime!!)
        }

        if (temporalQuery.attrs.isNotEmpty()) {
            criteriaStep = criteriaStep.and("attribute_name").`in`(temporalQuery.attrs)
        }

        // TODO we actually only support queries providing an entity id
        val results = if (entityTemporalProperty.observedBy != null)
            fromSelectSpec.matching(criteriaStep.and("observed_by").`is`(entityTemporalProperty.observedBy)).fetch().all()
        else
            fromSelectSpec.matching(criteriaStep).fetch().all()

        return results.collectList()
    }
}
