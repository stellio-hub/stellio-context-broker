package com.egm.datahub.context.search.service

import com.egm.datahub.context.search.model.NgsiLdObservation
import com.egm.datahub.context.search.model.Observation
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service

@Service
class ObservationService(
    private val databaseClient: DatabaseClient
)  {

    fun create(ngsiLdObservation: NgsiLdObservation) {
        val observation = Observation(
            observedBy = ngsiLdObservation.observedBy.target,
            observedAt = ngsiLdObservation.observedAt,
            value = ngsiLdObservation.value,
            unitCode = ngsiLdObservation.unitCode,
            latitude = ngsiLdObservation.location.value.coordinates[0],
            longitude = ngsiLdObservation.location.value.coordinates[1]
        )

        databaseClient.insert()
            .into(Observation::class.java)
            .using(observation)
            .then()
            .subscribe()
    }
}