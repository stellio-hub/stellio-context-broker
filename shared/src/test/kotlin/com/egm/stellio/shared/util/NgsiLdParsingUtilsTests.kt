package com.egm.stellio.shared.util

import junit.framework.TestCase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.springframework.core.io.ClassPathResource

class NgsiLdParsingUtilsTests {
    private val rawEntityWithPolygonLocation = """
            {
               "id":"urn:ngsi-ld:Apiary:XYZ01",
               "type":"Apiary",
               "name":{
                  "type":"Property",
                  "value":"ApiarySophia"
               },
               "location": {
                  "type": "GeoProperty",
                  "value": {
                     "type": "Polygon",
                     "coordinates": [
                         [100.0, 0.0],
                         [101.0, 0.0], 
                         [101.0, 1.0],
                         [100.0, 1.0],
                         [100.0, 0.0]
                     ]
                  }
               },
               "@context":[
                  "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
                  "https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/ed0b0103c8b498c034d8ad367d3494d02a9ad28b/apic.jsonld",
                  "https://gist.githubusercontent.com/bobeal/4a836c81b837673b12e9db9916b1cd35/raw/82fba02005f3fc572d60744e38f6591bbaa09d6d/egm.jsonld"
               ]
            } 
        """.trimIndent()

    private val rawEntityWithPointLocation = """
            {
               "id":"urn:ngsi-ld:Apiary:XYZ01",
               "type":"Apiary",
               "name":{
                  "type":"Property",
                  "value":"ApiarySophia"
               },
               "location": {
                  "type": "GeoProperty",
                  "value": {
                     "type": "Point",
                     "coordinates": [
                        24.30623,
                        60.07966
                     ]
                  }
               },
               "@context":[
                  "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
                  "https://gist.githubusercontent.com/bobeal/2e5905a069ad534b4919839b6b4c1245/raw/ed0b0103c8b498c034d8ad367d3494d02a9ad28b/apic.jsonld",
                  "https://gist.githubusercontent.com/bobeal/4a836c81b837673b12e9db9916b1cd35/raw/82fba02005f3fc572d60744e38f6591bbaa09d6d/egm.jsonld"
               ]
            } 
        """.trimIndent()

    private val parsedEntityWithPolygonLocation = NgsiLdParsingUtils.parseEntity(rawEntityWithPolygonLocation)
    private val parsedEntityWithPointLocation = NgsiLdParsingUtils.parseEntity(rawEntityWithPointLocation)

    @Test
    fun `it should parse the incoming temporal property to an observation`() {
        val jsonLdObservation = ClassPathResource("/ngsild/Observation.json")
            .inputStream.readBytes().toString(Charsets.UTF_8)

        val observation = NgsiLdParsingUtils.parseTemporalPropertyUpdate(jsonLdObservation)

        assertNotNull(observation)
        assertEquals("incoming", observation!!.attributeName)
        assertEquals(20.7, observation.value)
        assertEquals("CEL", observation.unitCode)
        assertEquals("urn:sosa:Sensor:10e2073a01080065", observation.observedBy)
        assertEquals("2019-10-18T07:31:39.770Z", observation.observedAt.toString())
        assertEquals(24.30623, observation.longitude)
        assertEquals(60.07966, observation.latitude)
    }

    @Test
    fun `it should return a null object if the incoming payload is invalid`() {
        val jsonLdObservation = ClassPathResource("/ngsild/ObservationWithoutUnitCode.json")
            .inputStream.readBytes().toString(Charsets.UTF_8)

        val observation = NgsiLdParsingUtils.parseTemporalPropertyUpdate(jsonLdObservation)

        assertNull(observation)
    }

    @Test
    fun `it should get Polygon location from an entity`() {

        val location = NgsiLdParsingUtils.getLocationFromEntity(parsedEntityWithPolygonLocation)

        TestCase.assertEquals(
            location, mapOf(
                "geometry" to "Polygon", "coordinates" to
                    listOf(
                        listOf(100.0, 0.0),
                        listOf(101.0, 0.0),
                        listOf(101.0, 1.0),
                        listOf(100.0, 1.0),
                        listOf(100.0, 0.0)
                    )
            )
        )
    }

    @Test
    fun `it should get Point location from an entity`() {

        val location = NgsiLdParsingUtils.getLocationFromEntity(parsedEntityWithPointLocation)

        TestCase.assertEquals(location, mapOf("geometry" to "Point", "coordinates" to listOf(24.30623, 60.07966)))
    }
}
