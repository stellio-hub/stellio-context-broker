package com.egm.datahub.context.registry.service

import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles
import java.time.format.DateTimeFormatter

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

        verify { neo4jService.updateEntityLastMeasure(match { observation ->
            observation.id == "urn:ngsi-ld:Observation:00YFZF" &&
                observation.type == "Observation" &&
                observation.unitCode == "%" &&
                observation.value == 50.0 &&
                observation.observedAt.format(DateTimeFormatter.ISO_INSTANT) == "2018-11-26T19:32:52Z" &&
                observation.observedBy.type == "Relationship" &&
                observation.observedBy.target == "urn:ngsi-ld:Sensor:013YFZ" &&
                observation.location.type == "GeoProperty" &&
                observation.location.value.type == "Point" &&
                observation.location.value.coordinates == listOf(-8.5, 41.2)
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
