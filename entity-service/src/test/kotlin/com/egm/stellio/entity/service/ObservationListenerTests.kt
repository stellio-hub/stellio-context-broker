package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.Observation
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Called
import io.mockk.Runs
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.just
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ObservationListener::class])
@ActiveProfiles("test")
class ObservationListenerTests {

    @Autowired
    private lateinit var observationListener: ObservationListener

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var ngsiLdParsingUtils: NgsiLdParsingUtils

    @Test
    fun `it should parse and transmit observations`() {
        val observation = ClassPathResource("/ngsild/aquac/Observation.json")

        every { ngsiLdParsingUtils.parseTemporalPropertyUpdate(any()) } returns gimmeTemporalProperty()
        every { entityService.updateEntityLastMeasure(any()) } just Runs

        observationListener.processMessage(observation.inputStream.readBytes().toString(Charsets.UTF_8))

        verify {
            entityService.updateEntityLastMeasure(match { observation ->
                observation.attributeName == "incoming" &&
                    observation.unitCode == "CEL" &&
                    observation.value == 20.7 &&
                    observation.observedAt.format(DateTimeFormatter.ISO_INSTANT) == "2019-10-18T07:31:39.770Z" &&
                    observation.observedBy == "urn:sosa:Sensor:10e2073a01080065" &&
                    observation.longitude == 24.30623 &&
                    observation.latitude == 60.07966
            })
        }
        confirmVerified(entityService)
    }

    @Test
    fun `it should parse and transmit observations without location because location may be null`() {
        val observation = ClassPathResource("/ngsild/aquac/ObservationWithoutLocation.json")

        every { ngsiLdParsingUtils.parseTemporalPropertyUpdate(any()) } returns gimmeTemporalProperty(withLocation = false)
        every { entityService.updateEntityLastMeasure(any()) } just Runs

        observationListener.processMessage(observation.inputStream.readBytes().toString(Charsets.UTF_8))

        verify {
            entityService.updateEntityLastMeasure(match { observation ->
                observation.attributeName == "incoming" &&
                    observation.unitCode == "CEL" &&
                    observation.value == 20.7 &&
                    observation.observedAt.format(DateTimeFormatter.ISO_INSTANT) == "2019-10-18T07:31:39.770Z" &&
                    observation.observedBy == "urn:sosa:Sensor:10e2073a01080065" &&
                    observation.latitude == null &&
                    observation.longitude == null
            })
        }
        confirmVerified(entityService)
    }

    @Test
    fun `it should ignore badly formed observations`() {
        val observation = ClassPathResource("/ngsild/aquac/Observation_missingAttributes.json")

        observationListener.processMessage(observation.inputStream.readBytes().toString(Charsets.UTF_8))

        verify { entityService wasNot Called }
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
            observedAt = ZonedDateTime.parse("2019-10-18T07:31:39.77Z"),
            latitude = location?.second,
            longitude = location?.first,
            observedBy = "urn:sosa:Sensor:10e2073a01080065"
        )
    }
}
