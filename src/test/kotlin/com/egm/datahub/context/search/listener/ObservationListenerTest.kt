package com.egm.datahub.context.search.listener

import com.egm.datahub.context.search.model.Observation
import com.egm.datahub.context.search.util.NgsiLdParsingUtils
import com.egm.datahub.context.search.service.ObservationService
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.core.io.ClassPathResource
import reactor.core.publisher.Mono
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ ObservationListener::class ])
@ActiveProfiles("test")
class ObservationListenerTest {

    @Autowired
    private lateinit var observationListener: ObservationListener

    @MockkBean
    private lateinit var observationService: ObservationService

    @MockkBean
    private lateinit var ngsiLdParsingService: NgsiLdParsingUtils

    @Test
    fun `it should not raise an exception with a correct ngsild observation`() {

        every { ngsiLdParsingService.parseTemporalPropertyUpdate(any()) } returns gimmeTemporalProperty()
        every { observationService.create(any()) } returns Mono.just(1)

        val jsonLdObservation = ClassPathResource("/ngsild/observation.jsonld")
        observationListener.processMessage(jsonLdObservation.inputStream.readBytes().toString(Charsets.UTF_8))

        verify {
            observationService.create(match { observation ->
                observation.attributeName == "incoming" &&
                observation.unitCode == "CEL" &&
                observation.value == 20.7 &&
                observation.observedAt.format(DateTimeFormatter.ISO_INSTANT) == "2019-10-18T07:31:39.770Z" &&
                observation.observedBy == "urn:sosa:Sensor:10e2073a01080065" &&
                observation.longitude == 24.30623 &&
                observation.latitude == 60.07966
            })
        }

        confirmVerified(observationService)
    }

    @Test
    fun `it should raise an exception and ignores the observation`() {

        val jsonLdObservation = ClassPathResource("/ngsild/observationWithoutUnitCode.jsonld")
        observationListener.processMessage(jsonLdObservation.inputStream.readBytes().toString(Charsets.UTF_8))

        verify { observationService wasNot Called }
    }

    @Test
    fun `it should accept the message without location because location may be null`() {

        every { ngsiLdParsingService.parseTemporalPropertyUpdate(any()) } returns gimmeTemporalProperty(withLocation = false)
        every { observationService.create(any()) } returns Mono.just(1)

        val jsonLdObservation = ClassPathResource("/ngsild/observationWithoutLocation.jsonld")
        observationListener.processMessage(jsonLdObservation.inputStream.readBytes().toString(Charsets.UTF_8))

        verify {
            observationService.create(match { observation ->
                observation.attributeName == "incoming" &&
                observation.unitCode == "CEL" &&
                observation.value == 20.7 &&
                observation.observedAt.format(DateTimeFormatter.ISO_INSTANT) == "2019-10-18T07:31:39.770Z" &&
                observation.observedBy == "urn:sosa:Sensor:10e2073a01080065" &&
                observation.latitude == null &&
                observation.longitude == null
            })
        }
        confirmVerified(observationService)
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