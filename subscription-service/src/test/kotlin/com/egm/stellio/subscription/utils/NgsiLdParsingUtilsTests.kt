package com.egm.stellio.subscription.utils

import junit.framework.TestCase.*
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ NgsiLdParsingUtils::class ])
@ActiveProfiles("test")
class NgsiLdParsingUtilsTests {

    private final val rawEntityWithPolygonLocation = """
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

    private final val rawEntityWithPointLocation = """
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
    fun `it should get Polygon location from an entity`() {

        val location = NgsiLdParsingUtils.getLocationFromEntity(parsedEntityWithPolygonLocation)

        assertEquals(location, mapOf("geometry" to "Polygon", "coordinates" to
                    listOf(listOf(100.0, 0.0), listOf(101.0, 0.0), listOf(101.0, 1.0), listOf(100.0, 1.0), listOf(100.0, 0.0))))
    }

    @Test
    fun `it should get Point location from an entity`() {

        val location = NgsiLdParsingUtils.getLocationFromEntity(parsedEntityWithPointLocation)

        assertEquals(location, mapOf("geometry" to "Point", "coordinates" to listOf(24.30623, 60.07966)))
    }
}