package com.egm.datahub.context.registry.service

import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ EntitiesListener::class ])
@ActiveProfiles("test")
class EntitiesListenerTests {

    @Autowired
    private lateinit var entitiesListener: EntitiesListener

    @MockkBean
    private lateinit var neo4jService: Neo4jService

    @Test
    fun `it should parse and transmit observations`() {
        val observation = ClassPathResource("/ngsild/aquac/Observation.json")

        every { neo4jService.updateEntityLastMeasure(any()) } just Runs

        entitiesListener.processMessage(observation.inputStream.readBytes().toString(Charsets.UTF_8))

        val coordinatesExpected = mapOf("type" to "Point", "coordinates" to listOf(40, 40))
        val locationExpected = mapOf("type" to "GeoProperty", "value" to coordinatesExpected)
        val observedByExpected = mapOf("type" to "Relationship", "object" to "urn:ngsi-ld:Sensor:01XYZ")
        val valuesExpected = mapOf("type" to "Property", "value" to 1, "unitCode" to "CEL", "observedAt" to "2000-11-26T19:32:52Z",
                                    "observedBy" to observedByExpected, "location" to locationExpected)

        verify { neo4jService.updateEntityLastMeasure(match { observation ->
                observation.key == "temperature" &&
                observation.value as Map<String, Any> == valuesExpected
        }) }
        confirmVerified(neo4jService)
    }

    @Test
    fun `it should ignore badly formed observations`() {
        val measure = ClassPathResource("/ngsild/aquac/Observation_missingAttributes.json")

        entitiesListener.processMessage(measure.inputStream.readBytes().toString(Charsets.UTF_8))

        verify { neo4jService wasNot Called }
    }
}
