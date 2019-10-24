package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.model.Observation
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class EntitiesListener(
    private val neo4jService: Neo4jService
) {

    private val logger = LoggerFactory.getLogger(EntitiesListener::class.java)

    private var mapper = jacksonObjectMapper()

    init {
        // TODO check if this registration is still required
        mapper.findAndRegisterModules()
    }

    /**
     * Measures are received under the following format
     *
     * {
     *   "id": "urn:sosa:Observation:1234",
     *   "type": "Observation",
     *   "observedBy": {
     *     "type": "Relationship",
     *     "object": "urn:sosa:Sensor:10e2073a01080065"
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
    @KafkaListener(topics = ["cim.observations"])
    fun processMessage(content: ConsumerRecord<Any, Any>) {
        try {
            val observation: Observation = mapper.readValue(content.value() as String, Observation::class.java)
            logger.debug("Parsed observation: $observation")
            neo4jService.updateEntityLastMeasure(observation)
        } catch (e: Exception) {
            logger.error("Received a non-parseable measure : ${content.value()}", e)
        }
    }
}
