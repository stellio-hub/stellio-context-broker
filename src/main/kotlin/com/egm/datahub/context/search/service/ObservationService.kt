package com.egm.datahub.context.search.service

import com.egm.datahub.context.search.model.*
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.query.Criteria.where
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class ObservationService(
    private val databaseClient: DatabaseClient,
    private val contextRegistryService: ContextRegistryService
) {

    fun create(ngsiLdObservation: NgsiLdObservation): Mono<Int> {
        val observation = Observation(
            observedBy = ngsiLdObservation.observedBy.target,
            observedAt = ngsiLdObservation.observedAt,
            value = ngsiLdObservation.value,
            unitCode = ngsiLdObservation.unitCode,
            latitude = ngsiLdObservation.location?.value?.coordinates?.get(0),
            longitude = ngsiLdObservation.location?.value?.coordinates?.get(1)
        )

        return databaseClient.insert()
            .into(Observation::class.java)
            .using(observation)
            .fetch()
            .rowsUpdated()
    }

    fun search(temporalQuery: TemporalQuery): Mono<Entity> {

        val fromSelectSpec = databaseClient
            .select()
            .from("observation")
            .project("value", "observed_at")

        val timeCriteriaStep = when {
            temporalQuery.timerel == TemporalQuery.Timerel.BEFORE -> where("observed_at").lessThan(temporalQuery.time)
            temporalQuery.timerel == TemporalQuery.Timerel.AFTER -> where("observed_at").greaterThan(temporalQuery.time)
            else -> where("observed_at").greaterThan(temporalQuery.time)
                .and("observed_at").lessThan(temporalQuery.endTime!!)
        }

        val results = if (temporalQuery.entityId != null)
            fromSelectSpec.matching(timeCriteriaStep.and("observed_by").`is`(temporalQuery.entityId)).fetch().all()
        else
            fromSelectSpec.matching(timeCriteriaStep).fetch().all()

        return results.collectList()
            .zipWith(contextRegistryService.getEntityById(temporalQuery.entityId!!))
            .map {
                val entity = it.t2
                entity.addTemporalValues("measures", it.t1)
                entity
            }
    }
}
