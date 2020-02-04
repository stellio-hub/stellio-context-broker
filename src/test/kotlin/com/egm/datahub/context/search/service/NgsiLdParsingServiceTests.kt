package com.egm.datahub.context.search.service

import com.egm.datahub.context.search.exception.InvalidNgsiLdPayloadException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ NgsiLdParsingService::class ])
class NgsiLdParsingServiceTests {

    @Autowired
    private lateinit var ngsiLdParsingService: NgsiLdParsingService

    @Test
    fun `test simple parsing of a NGSI-LD entity`() {
        val ngsiLdPayload = """
            {
              "@context": [
                "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
                "https://diatomic.eglobalmark.com/diatomic-context.jsonld"
              ],
              "id": "urn:diat:BeeHive:TESTC",
              "type": "BeeHive",
              "name": "ParisBeehive12",
              "connectsTo": {
                "type": "Relationship",
                "createdAt": "2010-10-26T21:32:52+02:00",
                "object": "urn:diat:Beekeeper:Pascal"
              }
            }
        """.trimIndent()

        val entity = ngsiLdParsingService.parse(ngsiLdPayload)

        assertEquals(2, entity.contexts.size)
        assertIterableEquals(
            listOf("http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld", "https://diatomic.eglobalmark.com/diatomic-context.jsonld"),
            entity.contexts)
        assertEquals("ParisBeehive12", entity.getPropertyValue("name"))
        assertEquals(3, entity.getRelationshipValue("connectsTo")?.size)
        assertTrue(entity.getRelationshipValue("connectsTo")?.containsKey("object")!!)
        assertEquals("urn:diat:Beekeeper:Pascal", entity.getRelationshipValue("connectsTo")?.get("object"))
    }

    @Test
    fun `it should parse the incoming temporal property to an observation`() {
        val jsonLdObservation = ClassPathResource("/ngsild/observation.jsonld")
            .inputStream.readBytes().toString(Charsets.UTF_8)

        val observation = ngsiLdParsingService.parseTemporalPropertyUpdate(jsonLdObservation)

        assertEquals("incoming", observation.attributeName)
        assertEquals(20.7, observation.value)
        assertEquals("CEL", observation.unitCode)
        assertEquals("urn:sosa:Sensor:10e2073a01080065", observation.observedBy)
        assertEquals("2019-10-18T07:31:39.770Z", observation.observedAt.toString())
        assertEquals(24.30623, observation.latitude)
        assertEquals(60.07966, observation.longitude)
    }

    @Test
    fun `it should throw an exception if the incoming payload is invalid`() {
        val jsonLdObservation = ClassPathResource("/ngsild/observationWithoutUnitCode.jsonld")
            .inputStream.readBytes().toString(Charsets.UTF_8)

        assertThrows<InvalidNgsiLdPayloadException> {
            ngsiLdParsingService.parseTemporalPropertyUpdate(jsonLdObservation)
        }
    }
}
