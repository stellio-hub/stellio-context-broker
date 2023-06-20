package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.model.LdContextNotAvailableException
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_EGM_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.addCoreContextIfMissing
import com.egm.stellio.shared.util.JsonLdUtils.compact
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonLdUtils.extractContextFromInput
import com.egm.stellio.shared.util.JsonLdUtils.extractRelationshipObject
import com.egm.stellio.shared.util.JsonLdUtils.getAttributeFromExpandedAttributes
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.http.MediaType

@OptIn(ExperimentalCoroutinesApi::class)
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
                "https://example.org/ngsi-ld/latest/commonTerms.jsonld",
                "https://example.org/ngsi-ld/latest/vehicle.jsonld",
                "https://example.org/ngsi-ld/latest/parking.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
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
                "https://example.org/ngsi-ld/latest/commonTerms.jsonld",
                "https://example.org/ngsi-ld/latest/vehicle.jsonld",
                "https://example.org/ngsi-ld/latest/parking.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            ]
        }
        """.trimIndent()

    private val normalizedMultiAttributeJson =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "speed": [
                {
                    "type": "Property",
                    "datasetId": "urn:ngsi-ld:Dataset:01",
                    "value": 10
                },
                {
                    "type": "Property",
                    "datasetId": "urn:ngsi-ld:Dataset:02",
                    "value": 11
                }
            ],
            "hasOwner": [
                {
                    "type": "Relationship",
                    "datasetId": "urn:ngsi-ld:Dataset:01",
                    "object": "urn:ngsi-ld:Person:John"
                },
                {
                    "type": "Relationship",
                    "datasetId": "urn:ngsi-ld:Dataset:02",
                    "object": "urn:ngsi-ld:Person:Jane"
                }
            ]
        }
        """.trimIndent()

    private val simplifiedMultiAttributeJson =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "speed": [ 10, 11 ],
            "hasOwner": [ "urn:ngsi-ld:Person:John", "urn:ngsi-ld:Person:Jane" ]
        }
        """.trimIndent()

    @Test
    fun `it should simplify a compacted entity`() {
        val normalizedMap = mapper.readValue(normalizedJson, Map::class.java)
        val simplifiedMap = mapper.readValue(simplifiedJson, Map::class.java)

        val resultMap = (normalizedMap as CompactedJsonLdEntity).toKeyValues()

        assertEquals(simplifiedMap, resultMap)
    }

    @Test
    fun `it should simplify a compacted entity with multi-attributes`() {
        val normalizedMap = mapper.readValue(normalizedMultiAttributeJson, Map::class.java)
        val simplifiedMap = mapper.readValue(simplifiedMultiAttributeJson, Map::class.java)

        val resultMap = (normalizedMap as CompactedJsonLdEntity).toKeyValues()

        assertEquals(simplifiedMap, resultMap)
    }

    @Test
    fun `it should filter a JSON-LD Map on the attributes specified as well as the mandatory attributes`() {
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
    fun `it should throw a LdContextNotAvailable exception if the provided JSON-LD context is not available`() =
        runTest {
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

            val exception = assertThrows<LdContextNotAvailableException> {
                expandJsonLdFragment(rawEntity.deserializeAsMap(), listOf("unknownContext"))
            }
            assertEquals(
                "Unable to load remote context (cause was: com.github.jsonldjava.core.JsonLdError: " +
                    "loading remote context failed: unknownContext)",
                exception.message
            )
        }

    @Test
    fun `it should throw a BadRequestData exception if the expanded JSON-LD fragment is empty`() = runTest {
        val rawEntity =
            """
            {
                "@context": [
                    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
                ]
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            expandJsonLdFragment(rawEntity, listOf(NGSILD_CORE_CONTEXT))
        }
        assertEquals(
            "Unable to expand input payload",
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
    fun `it should return the core context if the list of contexts is empty`() {
        val contexts = addCoreContextIfMissing(emptyList())
        assertEquals(1, contexts.size)
        assertEquals(listOf(NGSILD_CORE_CONTEXT), contexts)
    }

    @Test
    fun `it should move the core context at last position if it is not last in the list of contexts`() {
        val contexts = addCoreContextIfMissing(listOf(NGSILD_CORE_CONTEXT, APIC_COMPOUND_CONTEXT))
        assertEquals(2, contexts.size)
        assertEquals(listOf(APIC_COMPOUND_CONTEXT, NGSILD_CORE_CONTEXT), contexts)
    }

    @Test
    fun `it should add the core context at last position if it is not in the list of contexts`() {
        val contexts = addCoreContextIfMissing(listOf(NGSILD_EGM_CONTEXT))
        assertEquals(2, contexts.size)
        assertEquals(listOf(NGSILD_EGM_CONTEXT, NGSILD_CORE_CONTEXT), contexts)
    }

    @Test
    fun `it should not add the core context if it resolvable from the provided contexts`() {
        val contexts = addCoreContextIfMissing(
            listOf("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.4.jsonld")
        )
        assertEquals(1, contexts.size)
        assertEquals(listOf("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.4.jsonld"), contexts)
    }

    @Test
    fun `it should compact and return a JSON entity`() = runTest {
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
    fun `it should compact and return a JSON-LD entity`() = runTest {
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
                    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
                ]
            }
            """.trimIndent()

        val jsonLdEntity = JsonLdUtils.expandJsonLdEntity(entity, DEFAULT_CONTEXTS)
        val compactedEntity = compact(jsonLdEntity, DEFAULT_CONTEXTS)

        assertTrue(mapper.writeValueAsString(compactedEntity).matchContent(expectedEntity))
    }

    @Test
    fun `it should not find an unknown attribute instance in a list of attributes`() = runTest {
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
    fun `it should find an attribute instance from a list of attributes without multi-attributes`() = runTest {
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
        val expandedBrandName = expandJsonLdTerm("brandName", DEFAULT_CONTEXTS)

        assertNotNull(getAttributeFromExpandedAttributes(expandedAttributes, expandedBrandName, null))
        assertNull(
            getAttributeFromExpandedAttributes(expandedAttributes, expandedBrandName, "urn:datasetId".toUri())
        )
    }

    @Test
    fun `it should find an attribute instance from a list of attributes with multi-attributes`() = runTest {
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
        val expandedBrandName = expandJsonLdTerm("brandName", DEFAULT_CONTEXTS)
        val expandedName = expandJsonLdTerm("name", DEFAULT_CONTEXTS)

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

    @Test
    fun `it should find a JSON-LD @context in an input map`() {
        val input = mapOf(
            "id" to "urn:ngsi-ld:Entity:1",
            "@context" to "https://some.context"
        )

        assertEquals(listOf("https://some.context"), extractContextFromInput(input))
    }

    @Test
    fun `it should find a list of JSON-LD @contexts in an input map`() {
        val input = mapOf(
            "id" to "urn:ngsi-ld:Entity:1",
            "@context" to listOf("https://some.context", "https://some.context.2")
        )

        assertEquals(listOf("https://some.context", "https://some.context.2"), extractContextFromInput(input))
    }

    @Test
    fun `it should return an empty list if no JSON-LD @context was found an input map`() {
        val input = mapOf(
            "id" to "urn:ngsi-ld:Entity:1"
        )

        assertEquals(emptyList<String>(), extractContextFromInput(input))
    }

    @Test
    fun `it should add createdAt information into an attribute`() {
        val attrPayload = mapOf(
            "type" to "Property",
            "value" to 12.0
        )

        val attrPayloadWithSysAttrs = attrPayload.addSysAttrs(true, ngsiLdDateTime(), null)

        assertTrue(attrPayloadWithSysAttrs.containsKey(JsonLdUtils.NGSILD_CREATED_AT_PROPERTY))
        assertFalse(attrPayloadWithSysAttrs.containsKey(JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY))
    }

    @Test
    fun `it should add createdAt and modifiedAt information into an attribute`() {
        val attrPayload = mapOf(
            "type" to "Property",
            "value" to 12.0
        )

        val attrPayloadWithSysAttrs = attrPayload.addSysAttrs(true, ngsiLdDateTime(), ngsiLdDateTime())

        assertTrue(attrPayloadWithSysAttrs.containsKey(JsonLdUtils.NGSILD_CREATED_AT_PROPERTY))
        assertTrue(attrPayloadWithSysAttrs.containsKey(JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY))
    }
}
