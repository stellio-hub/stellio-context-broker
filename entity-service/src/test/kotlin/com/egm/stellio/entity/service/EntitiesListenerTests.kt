package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.Observation
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ EntitiesListener::class ])
@ActiveProfiles("test")
class EntitiesListenerTests {

    @Autowired
    private lateinit var entitiesListener: EntitiesListener

    @MockkBean
    private lateinit var neo4jService: Neo4jService

    @MockkBean
    private lateinit var ngsiLdParsingUtils: NgsiLdParsingUtils

    @Test
    fun `it should parse and transmit observations`() {
        val observation = ClassPathResource("/ngsild/aquac/Observation.json")

        every { ngsiLdParsingUtils.parseTemporalPropertyUpdate(any()) } returns gimmeTemporalProperty()
        every { neo4jService.updateEntityLastMeasure(any()) } just Runs

        entitiesListener.processMessage(observation.inputStream.readBytes().toString(Charsets.UTF_8))

        verify { neo4jService.updateEntityLastMeasure(match { observation ->
            observation.attributeName == "incoming" &&
            observation.unitCode == "CEL" &&
            observation.value == 20.7 &&
            observation.observedAt.format(DateTimeFormatter.ISO_INSTANT) == "2019-10-18T07:31:39.770Z" &&
            observation.observedBy == "urn:sosa:Sensor:10e2073a01080065" &&
            observation.longitude == 24.30623 &&
            observation.latitude == 60.07966
        }) }
        confirmVerified(neo4jService)
    }

    @Test
    fun `it should parse and transmit observations without location because location may be null`() {
        val observation = ClassPathResource("/ngsild/aquac/ObservationWithoutLocation.json")

        every { ngsiLdParsingUtils.parseTemporalPropertyUpdate(any()) } returns gimmeTemporalProperty(withLocation = false)
        every { neo4jService.updateEntityLastMeasure(any()) } just Runs

        entitiesListener.processMessage(observation.inputStream.readBytes().toString(Charsets.UTF_8))

        verify { neo4jService.updateEntityLastMeasure(match { observation ->
            observation.attributeName == "incoming" &&
                    observation.unitCode == "CEL" &&
                    observation.value == 20.7 &&
                    observation.observedAt.format(DateTimeFormatter.ISO_INSTANT) == "2019-10-18T07:31:39.770Z" &&
                    observation.observedBy == "urn:sosa:Sensor:10e2073a01080065" &&
                    observation.latitude == null &&
                    observation.longitude == null
        }) }
        confirmVerified(neo4jService)
    }

    @Test
    fun `it should ignore badly formed observations`() {
        val observation = ClassPathResource("/ngsild/aquac/Observation_missingAttributes.json")

        entitiesListener.processMessage(observation.inputStream.readBytes().toString(Charsets.UTF_8))

        verify { neo4jService wasNot Called }
    }

    private fun gimmeTemporalProperty(withLocation: Boolean = true): Observation {
        val location =
            if (withLocation)
                Pair(24.30623, 60.07966)
            else null

        return Observation(
            attributeName = "incoming",
            value = 20.7,
            unitCode = "CEL",
            observedAt = OffsetDateTime.parse("2019-10-18T07:31:39.77Z"),
            latitude = location?.second,
            longitude = location?.first,
            observedBy = "urn:sosa:Sensor:10e2073a01080065"
        )
    }
}
