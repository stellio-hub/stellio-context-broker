package com.egm.stellio.entity.service

import com.egm.stellio.entity.util.NgsiLdParsingUtils
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

    /**
     * Measures are received under the following format
     * {
     *   "incoming": {
     *      "type": "Property",
     *      "value": 1500,
     *      "unitCode": "<UN/CEFACT code>",
     *      "observedAt": "2019-12-30...",
     *      "observedBy": {
     *      "type": "Relationship",
     *      "object": "urn:ngsi-ld:Sensor:0123-3456-..."
     *      },
     *      "location": {
     *          "type": "GeoProperty",
     *          "value": {
     *              "type": "Point",
     *              "coordinates": [
     *                  24.30623,
     *                  60.07966
     *              ]
     *          }
     *      }
     *  }
     * }
     */

    @StreamListener("cim.observations")
    fun processMessage(content: String) {
        try {
            val observation = NgsiLdParsingUtils.parseTemporalPropertyUpdate(content)
            logger.debug("Parsed observation: $observation")
            neo4jService.updateEntityLastMeasure(observation)
        } catch (e: Exception) {
            logger.error("Received a non-parseable measure : $content", e)
        }
    }
}
