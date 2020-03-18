package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.InvalidNgsiLdPayloadException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.core.io.ClassPathResource

class NgsiLdParsingUtilsTests {

    @Test
    fun `it should parse the incoming temporal property to an observation`() {
        val jsonLdObservation = ClassPathResource("/ngsild/Observation.json")
            .inputStream.readBytes().toString(Charsets.UTF_8)

        val observation = NgsiLdParsingUtils.parseTemporalPropertyUpdate(jsonLdObservation)

        assertEquals("incoming", observation.attributeName)
        assertEquals(20.7, observation.value)
        assertEquals("CEL", observation.unitCode)
        assertEquals("urn:sosa:Sensor:10e2073a01080065", observation.observedBy)
        assertEquals("2019-10-18T07:31:39.770Z", observation.observedAt.toString())
        assertEquals(24.30623, observation.longitude)
        assertEquals(60.07966, observation.latitude)
    }

    @Test
    fun `it should throw an exception if the incoming payload is invalid`() {
        val jsonLdObservation = ClassPathResource("/ngsild/ObservationWithoutUnitCode.json")
            .inputStream.readBytes().toString(Charsets.UTF_8)

        assertThrows<InvalidNgsiLdPayloadException> {
            NgsiLdParsingUtils.parseTemporalPropertyUpdate(jsonLdObservation)
        }
    }
}
