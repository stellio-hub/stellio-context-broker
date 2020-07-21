package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.Observation
import com.egm.stellio.shared.util.JsonUtils.parseJsonContent
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.stereotype.Component
import java.time.ZonedDateTime

@Component
@EnableBinding(ObservationsSink::class)
class EntitiesListener(
    private val entityService: EntityService
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
            val observation = parseTemporalPropertyUpdate(content)
            observation?.let {
                logger.debug("Parsed observation: $observation")
                entityService.updateEntityLastMeasure(observation)
            }
        } catch (e: Exception) {
            logger.error("Received a non-parseable measure : $content", e)
        }
    }

    fun parseTemporalPropertyUpdate(content: String): Observation? {
        val rawParsedData = parseJsonContent(content)
        val propertyName = rawParsedData.fieldNames().next()
        val propertyValues = rawParsedData[propertyName]

        logger.debug("Received property $rawParsedData (values: $propertyValues)")
        val isContentValid = listOf("type", "value", "unitCode", "observedAt", "observedBy")
            .map { propertyValues.has(it) }
            .all { it }
        if (!isContentValid) {
            logger.warn("Received content is not a temporal property, ignoring it")
            return null
        }

        val location =
            if (propertyValues["location"] != null) {
                val coordinates = propertyValues["location"]["value"]["coordinates"].asIterable()
                // as per https://tools.ietf.org/html/rfc7946#appendix-A.1, first is longitude, second is latitude
                Pair(coordinates.first().asDouble(), coordinates.last().asDouble())
            } else {
                null
            }

        return Observation(
            attributeName = propertyName,
            observedBy = propertyValues["observedBy"]["object"].asText(),
            observedAt = ZonedDateTime.parse(propertyValues["observedAt"].asText()),
            value = propertyValues["value"].asDouble(),
            unitCode = propertyValues["unitCode"].asText(),
            latitude = location?.second,
            longitude = location?.first
        )
    }
}
