package com.egm.datahub.context.search.util

import com.egm.datahub.context.search.exception.InvalidNgsiLdPayloadException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ NgsiLdParsingUtils::class ])
class NgsiLdParsingUtilsTests {

    @Test
    fun `it should parse the incoming temporal property to an observation`() {
        val jsonLdObservation = ClassPathResource("/ngsild/observation.jsonld")
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
        val jsonLdObservation = ClassPathResource("/ngsild/observationWithoutUnitCode.jsonld")
            .inputStream.readBytes().toString(Charsets.UTF_8)

        assertThrows<InvalidNgsiLdPayloadException> {
            NgsiLdParsingUtils.parseTemporalPropertyUpdate(jsonLdObservation)
        }
    }
}
