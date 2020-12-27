package com.egm.stellio.entity.service

import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toNgsiLdFormat
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Called
import io.mockk.Runs
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.just
import io.mockk.verify
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ObservationListener::class])
@ActiveProfiles("test")
class ObservationListenerTests {

    @Autowired
    private lateinit var observationListener: ObservationListener

    @MockkBean
    private lateinit var entityService: EntityService

    private val observationDate = "2019-10-18T07:31:39.770Z"
    private val observedByEntityUri = "urn:sosa:Sensor:10e2073a01080065".toUri()

    @Test
    fun `it should parse the incoming temporal property to an observation`() {
        val jsonLdObservation = loadSampleData("aquac/Observation.json")

        val observation = observationListener.parseTemporalPropertyUpdate(jsonLdObservation)

        assertNotNull(observation)
        assertEquals("incoming", observation!!.attributeName)
        assertEquals(20.7, observation.value)
        assertEquals("CEL", observation.unitCode)
        assertEquals(observedByEntityUri, observation.observedBy)
        assertEquals(observationDate, observation.observedAt.toNgsiLdFormat())
        assertEquals(24.30623, observation.longitude)
        assertEquals(60.07966, observation.latitude)
    }

    @Test
    fun `it should return a null object if the incoming payload is invalid`() {
        val jsonLdObservation = loadSampleData("aquac/ObservationWithoutUnitCode.json")

        val observation = observationListener.parseTemporalPropertyUpdate(jsonLdObservation)

        assertNull(observation)
    }

    @Test
    fun `it should parse and transmit observations`() {
        val observation = loadSampleData("aquac/Observation.json")

        every { entityService.updateEntityLastMeasure(any()) } just Runs

        observationListener.processMessage(observation)

        verify {
            entityService.updateEntityLastMeasure(
                match { observation ->
                    observation.attributeName == "incoming" &&
                        observation.unitCode == "CEL" &&
                        observation.value == 20.7 &&
                        observation.observedAt.toNgsiLdFormat() == observationDate &&
                        observation.observedBy == observedByEntityUri &&
                        observation.longitude == 24.30623 &&
                        observation.latitude == 60.07966
                }
            )
        }
        confirmVerified(entityService)
    }

    @Test
    fun `it should parse and transmit observations without location because location may be null`() {
        val observation = loadSampleData("aquac/ObservationWithoutLocation.json")

        every { entityService.updateEntityLastMeasure(any()) } just Runs

        observationListener.processMessage(observation)

        verify {
            entityService.updateEntityLastMeasure(
                match { observation ->
                    observation.attributeName == "incoming" &&
                        observation.unitCode == "CEL" &&
                        observation.value == 20.7 &&
                        observation.observedAt.toNgsiLdFormat() == observationDate &&
                        observation.observedBy == observedByEntityUri &&
                        observation.latitude == null &&
                        observation.longitude == null
                }
            )
        }
        confirmVerified(entityService)
    }

    @Test
    fun `it should ignore badly formed observations`() {
        val observation = loadSampleData("aquac/Observation_missingAttributes.json")

        observationListener.processMessage(observation)

        verify { entityService wasNot Called }
    }
}
