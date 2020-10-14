package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.model.InvalidRequestException
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class JsonLdUtilsTests {

    private val normalizedJson =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "brandName": {
                "type": "Property",
                "value": "Mercedes"
            },
            "isParked": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:OffStreetParking:Downtown1",
                "observedAt": "2017-07-29T12:00:04Z",
                "providedBy": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Person:Bob"
                    }
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
            "@context": [
                "http://example.org/ngsi-ld/latest/commonTerms.jsonld",
                "http://example.org/ngsi-ld/latest/vehicle.jsonld",
                "http://example.org/ngsi-ld/latest/parking.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            ]
        }
        """.trimIndent()

    private val simplifiedJson =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "brandName": "Mercedes",
            "isParked": "urn:ngsi-ld:OffStreetParking:Downtown1",
            "location": {
             "type": "Point",
             "coordinates": [
                24.30623,
                60.07966
             ]
           },
            "@context": [
                "http://example.org/ngsi-ld/latest/commonTerms.jsonld",
                "http://example.org/ngsi-ld/latest/vehicle.jsonld",
                "http://example.org/ngsi-ld/latest/parking.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            ]
        }
        """.trimIndent()

    @Test
    fun `it should simplify a JSON-LD Map`() {
        val mapper = ObjectMapper()
        val normalizedMap = mapper.readValue(normalizedJson, Map::class.java)
        val simplifiedMap = mapper.readValue(simplifiedJson, Map::class.java)

        val resultMap = (normalizedMap as CompactedJsonLdEntity).toKeyValues()

        Assertions.assertEquals(simplifiedMap, resultMap)
    }

    @Test
    fun `it should filter a JSON-LD Map on the attributes specified as well as the mandatory attributes`() {
        val mapper = ObjectMapper()
        val normalizedMap = mapper.readValue(normalizedJson, Map::class.java)

        val resultMap = JsonLdUtils.filterCompactedEntityOnAttributes(
            normalizedMap as CompactedJsonLdEntity,
            setOf("brandName", "location")
        )

        Assertions.assertTrue(resultMap.containsKey("id"))
        Assertions.assertTrue(resultMap.containsKey("type"))
        Assertions.assertTrue(resultMap.containsKey("@context"))
        Assertions.assertTrue(resultMap.containsKey("brandName"))
        Assertions.assertTrue(resultMap.containsKey("location"))
        Assertions.assertFalse(resultMap.containsKey("isParked"))
    }

    @Test
    fun `it should throw an InvalidRequest exception if the JSON-LD fragment is not a valid JSON document`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",,
                "type": "Device"
            }
            """.trimIndent()

        val exception = assertThrows<InvalidRequestException> {
            parseAndExpandJsonLdFragment(rawEntity)
        }
        Assertions.assertNotNull(exception.message)
    }

    @Test
    fun `it should throw a BadRequestData exception if the JSON-LD fragment is not a valid JSON-LD document`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "@context": [
                    "unknownContext"
                ]
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            parseAndExpandJsonLdFragment(rawEntity)
        }
        Assertions.assertEquals(
            "Unexpected error while parsing payload : loading remote context failed: unknownContext",
            exception.message
        )
    }
}
