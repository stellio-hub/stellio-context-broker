package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.compact
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonLdUtils.extractRelationshipObject
import com.egm.stellio.shared.util.JsonLdUtils.getAttributeFromExpandedAttributes
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.http.MediaType

class JsonLdUtilsTests {

    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

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

        assertEquals(simplifiedMap, resultMap)
    }

    @Test
    fun `it should filter a JSON-LD Map on the attributes specified as well as the mandatory attributes`() {
        val mapper = ObjectMapper()
        val normalizedMap = mapper.readValue(normalizedJson, Map::class.java)

        val resultMap = JsonLdUtils.filterCompactedEntityOnAttributes(
            normalizedMap as CompactedJsonLdEntity,
            setOf("brandName", "location")
        )

        assertTrue(resultMap.containsKey("id"))
        assertTrue(resultMap.containsKey("type"))
        assertTrue(resultMap.containsKey("@context"))
        assertTrue(resultMap.containsKey("brandName"))
        assertTrue(resultMap.containsKey("location"))
        assertFalse(resultMap.containsKey("isParked"))
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
        assertEquals(
            "Unexpected error while parsing payload : Unexpected character (',' (code 44)): was expecting" +
                " double-quote to start field name\n at [Source: (BufferedReader); line: 2, column: 39]",
            exception.message
        )
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
        assertEquals(
            "Unexpected error while parsing payload : loading remote context failed: unknownContext",
            exception.message
        )
    }

    @Test
    fun `it should throw a BadRequestData exception if the expanded JSON-LD fragment is empty`() {
        val rawEntity =
            """
            {
                "@context": [
                    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
                ]
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            parseAndExpandJsonLdFragment(rawEntity)
        }
        assertEquals(
            "Unable to parse input payload",
            exception.message
        )
    }

    @Test
    fun `it should return an error if a relationship has no object field`() {
        val relationshipValues = mapOf(
            "value" to listOf("something")
        )

        val result = extractRelationshipObject("isARelationship", relationshipValues)
        assertTrue(result.isLeft())
        result.mapLeft {
            assertEquals("Relationship isARelationship does not have an object field", it.message)
        }
    }

    @Test
    fun `it should return an error if a relationship has an empty object`() {
        val relationshipValues = mapOf(
            NGSILD_RELATIONSHIP_HAS_OBJECT to emptyList<Any>()
        )

        val result = extractRelationshipObject("isARelationship", relationshipValues)
        assertTrue(result.isLeft())
        result.mapLeft {
            assertEquals("Relationship isARelationship is empty", it.message)
        }
    }

    @Test
    fun `it should return an error if a relationship has an invalid object type`() {
        val relationshipValues = mapOf(
            NGSILD_RELATIONSHIP_HAS_OBJECT to listOf("invalid")
        )

        val result = extractRelationshipObject("isARelationship", relationshipValues)
        assertTrue(result.isLeft())
        result.mapLeft {
            assertEquals("Relationship isARelationship has an invalid object type: class java.lang.String", it.message)
        }
    }

    @Test
    fun `it should return an error if a relationship has object without id`() {
        val relationshipValues = mapOf(
            NGSILD_RELATIONSHIP_HAS_OBJECT to listOf(
                mapOf("@value" to "urn:ngsi-ld:T:misplacedRelationshipObject")
            )
        )

        val result = extractRelationshipObject("isARelationship", relationshipValues)
        assertTrue(result.isLeft())
        result.mapLeft {
            assertEquals("Relationship isARelationship has an invalid or no object id: null", it.message)
        }
    }

    @Test
    fun `it should extract the target object of a relationship`() {
        val relationshipObjectId = "urn:ngsi-ld:T:1"
        val relationshipValues = mapOf(
            NGSILD_RELATIONSHIP_HAS_OBJECT to listOf(
                mapOf("@id" to relationshipObjectId)
            )
        )

        val result = extractRelationshipObject("isARelationship", relationshipValues)
        assertTrue(result.isRight())
        result.map {
            assertEquals(relationshipObjectId.toUri(), it)
        }
    }

    @Test
    fun `it should compact and return a JSON entity`() {
        val entity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device"
            }
            """.trimIndent()

        val jsonLdEntity = JsonLdUtils.expandJsonLdEntity(entity, DEFAULT_CONTEXTS)
        val compactedEntity = compact(jsonLdEntity, DEFAULT_CONTEXTS, MediaType.APPLICATION_JSON)

        assertTrue(mapper.writeValueAsString(compactedEntity).matchContent(entity))
    }

    @Test
    fun `it should compact and return a JSON-LD entity`() {
        val entity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device"
            }
            """.trimIndent()
        val expectedEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "@context":[
                    "https://fiware.github.io/data-models/context.jsonld",
                    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
                ]
            }
            """.trimIndent()

        val jsonLdEntity = JsonLdUtils.expandJsonLdEntity(entity, DEFAULT_CONTEXTS)
        val compactedEntity = compact(jsonLdEntity, DEFAULT_CONTEXTS)

        assertTrue(mapper.writeValueAsString(compactedEntity).matchContent(expectedEntity))
    }

    @Test
    fun `it should not find an unknown attribute instance in a list of attributes`() {
        val entityFragment =
            """
            {
                "brandName": {
                    "value": "a new value"
                }            
            }
            """.trimIndent()

        val expandedAttributes = expandJsonLdFragment(entityFragment, DEFAULT_CONTEXTS)
        assertNull(getAttributeFromExpandedAttributes(expandedAttributes, "unknownAttribute", null))
    }

    @Test
    fun `it should find an attribute instance from a list of attributes without multi-attributes`() {
        val entityFragment =
            """
            {
                "brandName": {
                    "value": "a new value",
                    "observedAt": "2021-03-16T00:00:00.000Z"
                },
                "name": {
                    "value": 12
                }
            }
            """.trimIndent()

        val expandedAttributes = expandJsonLdFragment(entityFragment, DEFAULT_CONTEXTS)
        val expandedBrandName = expandJsonLdKey("brandName", DEFAULT_CONTEXTS)!!

        assertNotNull(getAttributeFromExpandedAttributes(expandedAttributes, expandedBrandName, null))
        assertNull(
            getAttributeFromExpandedAttributes(expandedAttributes, expandedBrandName, "urn:datasetId".toUri())
        )
    }

    @Test
    fun `it should find an attribute instance from a list of attributes with multi-attributes`() {
        val entityFragment =
            """
            {
                "brandName": [{
                    "value": "a new value",
                    "observedAt": "2021-03-16T00:00:00.000Z"
                },
                {
                    "value": "a new value",
                    "observedAt": "2021-03-16T00:00:00.000Z",
                    "datasetId": "urn:datasetId:1"
                }],
                "name": {
                    "value": 12,
                    "datasetId": "urn:datasetId:1"
                }
            }
            """.trimIndent()

        val expandedAttributes = expandJsonLdFragment(entityFragment, DEFAULT_CONTEXTS)
        val expandedBrandName = expandJsonLdKey("brandName", DEFAULT_CONTEXTS)!!
        val expandedName = expandJsonLdKey("name", DEFAULT_CONTEXTS)!!

        assertNotNull(getAttributeFromExpandedAttributes(expandedAttributes, expandedBrandName, null))
        assertNotNull(
            getAttributeFromExpandedAttributes(expandedAttributes, expandedBrandName, "urn:datasetId:1".toUri())
        )
        assertNull(
            getAttributeFromExpandedAttributes(expandedAttributes, expandedBrandName, "urn:datasetId:2".toUri())
        )
        assertNotNull(
            getAttributeFromExpandedAttributes(expandedAttributes, expandedName, "urn:datasetId:1".toUri())
        )
        assertNull(getAttributeFromExpandedAttributes(expandedAttributes, expandedName, null))
    }
}
