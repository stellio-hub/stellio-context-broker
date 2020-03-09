package com.egm.stellio.search.listener

import com.egm.stellio.search.util.NgsiLdParsingUtils
import com.egm.stellio.search.service.ObservationService
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.stereotype.Component

@Component
@EnableBinding(ObservationsSink::class)
class ObservationListener(
    private val observationService: ObservationService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Observations are received under the following format
     *
     * {
     *   "incoming": {
     *     "type": "Property",
     *     "value": 1500,
     *     "unitCode": "<UN/CEFACT code>",
     *     "observedAt": "2019-12-30...",
     *     "observedBy": {
     *       "type": "Relationship",
     *       "object": "urn:ngsi-ld:Sensor:0123-3456-..."
     *     }
     *   }
     * }
     */
    @StreamListener("cim.observations")
    fun processMessage(content: String) {
        try {
            val observation = NgsiLdParsingUtils.parseTemporalPropertyUpdate(content)
            logger.debug("Parsed observation: $observation")
            observationService.create(observation)
                .doOnError {
                    logger.error("Failed to persist observation, ignoring it", it)
                }
                .doOnNext {
                    logger.debug("Created observation ($it)")
                }
                .subscribe()
        } catch (e: Exception) {
            logger.error("Received a non-parseable measure : $content", e) }
    }
}
