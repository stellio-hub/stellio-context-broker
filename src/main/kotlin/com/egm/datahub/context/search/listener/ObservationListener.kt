package com.egm.datahub.context.search.listener

import com.egm.datahub.context.search.model.NgsiLdObservation
import com.egm.datahub.context.search.service.ObservationService
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.stereotype.Component

@Component
@EnableBinding(ObservationsSink::class)
class ObservationListener(
    private val observationService: ObservationService
) {

    private val logger = LoggerFactory.getLogger(ObservationListener::class.java)
    private var mapper = jacksonObjectMapper()

    init {
        // TODO check if this registration is still required
        mapper.findAndRegisterModules()
    }
    /**
     * Observations are received under the following format
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
    @StreamListener("cim.observations")
    fun processMessage(content: String) {
        try {
            val ngsiLdObservation: NgsiLdObservation = mapper.readValue(content, NgsiLdObservation::class.java)
            logger.debug("Parsed observation: $ngsiLdObservation")
            observationService.create(ngsiLdObservation)
        } catch (e: Exception) {
            logger.error("Received a non-parseable measure : $content", e) }
    }
}
