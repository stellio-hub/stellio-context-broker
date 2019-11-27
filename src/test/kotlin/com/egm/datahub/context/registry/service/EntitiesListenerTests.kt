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
        val observation = ClassPathResource("/ngsild/observation.json")

        every { neo4jService.updateEntityLastMeasure(any()) } just Runs

        entitiesListener.processMessage(observation.inputStream.readBytes().toString(Charsets.UTF_8))

        verify { neo4jService.updateEntityLastMeasure(match { observation ->
            observation.id == "urn:ngsi-ld:Observation:111122223333" &&
                observation.type == "Observation" &&
                observation.unitCode == "CEL" &&
                observation.value == 20.7 &&
                observation.observedAt.format(DateTimeFormatter.ISO_INSTANT) == "2019-10-18T07:31:39.770Z" &&
                observation.observedBy.type == "Relationship" &&
                observation.observedBy.target == "urn:ngsi-ld:Sensor:10e2073a01080065" &&
                observation.location.type == "GeoProperty" &&
                observation.location.value.type == "Point" &&
                observation.location.value.coordinates == listOf(24.30623, 60.07966)
        }) }
        confirmVerified(neo4jService)
    }

    @Test
    fun `it should ignore badly formed observations`() {
        val measure = ClassPathResource("/ngsild/observation_door.json")

        entitiesListener.processMessage(measure.inputStream.readBytes().toString(Charsets.UTF_8))

        verify { neo4jService wasNot Called }
    }
}
