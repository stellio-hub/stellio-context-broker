package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.shared.model.Observation
import org.springframework.data.r2dbc.core.DatabaseClient
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

        var selectQuery =
            if (temporalQuery.timeBucket != null)
                """
                    SELECT time_bucket('${temporalQuery.timeBucket}', observed_at) as time_bucket, 
                           ${temporalQuery.aggregate}(value) as value,
                           attribute_name
                    FROM observation
                """.trimIndent()
            else
                """
                    SELECT observed_at, value, attribute_name
                    FROM observation
                """.trimIndent()

        selectQuery = when (temporalQuery.timerel) {
            TemporalQuery.Timerel.BEFORE -> selectQuery.plus(" WHERE observed_at < '${temporalQuery.time}'")
            TemporalQuery.Timerel.AFTER -> selectQuery.plus(" WHERE observed_at > '${temporalQuery.time}'")
            else -> selectQuery.plus(" WHERE observed_at > '${temporalQuery.time}' AND observed_at < '${temporalQuery.endTime}'")
        }

        if (temporalQuery.attrs.isNotEmpty()) {
            val attrsList = temporalQuery.attrs.joinToString(",") { "'$it'" }
            selectQuery = selectQuery.plus(" AND attribute_name in ($attrsList)")
        }

        // TODO we actually only support queries providing an entity id
        if (entityTemporalProperty.observedBy != null)
            selectQuery = selectQuery.plus(" AND observed_by = '${entityTemporalProperty.observedBy}'")

        if (temporalQuery.timeBucket != null)
            selectQuery = selectQuery.plus(" GROUP BY time_bucket, attribute_name")

        return databaseClient.execute(selectQuery)
            .fetch()
            .all()
            .collectList()
    }
}
