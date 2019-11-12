package com.egm.datahub.context.search.service

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest

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
}
