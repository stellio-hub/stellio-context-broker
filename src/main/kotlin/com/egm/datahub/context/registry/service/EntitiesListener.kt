package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.model.Observation
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.stereotype.Component

@Component
@EnableBinding(ObservationsSink::class)
class EntitiesListener(
    private val neo4jService: Neo4jService
) {

    private val logger = LoggerFactory.getLogger(EntitiesListener::class.java)

    private var mapper = jacksonObjectMapper()

    init {
        // needed because it register the module necessary to parse dates
        // TODO make it an injectable bean to avoid local definitions everywhere
        mapper.findAndRegisterModules()
    }

    /**
     * Measures are received under the following format
     *
     * {
     *   "id": "urn:ngsi-ld:Observation:1234",
     *   "type": "Observation",
     *   "observedBy": {
     *     "type": "Relationship",
     *     "object": "urn:ngsi-ld:Sensor:10e2073a01080065"
     *   },
     *   "location": {
     *     "type": "GeoProperty",
     *     "value": {
     *       "type": "Point",
     *       "coordinates": [
     *         24.30623,
     *         60.07966
     *       ]
     *     }
     *   },
     *   "unitCode": "CEL",
     *   "value": 20.7,
     *   "observedAt": "2019-10-18T07:31:39.77Z"
     * }
     */
    @StreamListener("cim.observations")
    fun processMessage(content: String) {
        try {
            val observation: Observation = mapper.readValue(content, Observation::class.java)
            logger.debug("Parsed observation: $observation")
            neo4jService.updateEntityLastMeasure(observation)
        } catch (e: Exception) {
            logger.error("Received a non-parseable measure : $content", e)
        }
    }
}
