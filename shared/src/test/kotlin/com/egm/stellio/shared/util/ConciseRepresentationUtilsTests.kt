package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.ConciseRepresentationUtils.normalizeAttributeFragment
import com.egm.stellio.shared.util.ConciseRepresentationUtils.normalizeEntityFragment
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import org.junit.jupiter.api.Test

class ConciseRepresentationUtilsTests {

    @Test
    fun `normalizeAttributeFragment should wrap a scalar String as a Property`() {
        val expectedNormalizedRepresentation = """
            {
                "type": "Property",
                "value": "Paris"
            }
        """.trimIndent()

        val result = normalizeAttributeFragment("Paris")

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should wrap a scalar Number as a Property`() {
        val expectedNormalizedRepresentation = """
            {
                "type": "Property",
                "value": 42.5
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(42.5)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should wrap a scalar Boolean as a Property`() {
        val expectedNormalizedRepresentation = """
            {
                "type": "Property",
                "value": true
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(true)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should wrap a bare GeoJSON Point map as a GeoProperty`() {
        val geoJson = mapOf(
            "type" to "Point",
            "coordinates" to listOf(24.30623, 60.07966)
        )

        val expectedNormalizedRepresentation = """
            {
                "type": "GeoProperty",
                "value": {
                    "type": "Point",
                    "coordinates": [24.30623, 60.07966]
                }
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(geoJson)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should add Relationship type when object key is present`() {
        val conciseRel = mapOf("object" to "urn:ngsi-ld:Entity:01")

        val expectedNormalizedRepresentation = """
            {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Entity:01"
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(conciseRel)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should add JsonProperty type when json key is present`() {
        val conciseJsonProp = mapOf("json" to mapOf("key" to "value"))

        val expectedNormalizedRepresentation = """
            {
                "type": "JsonProperty",
                "json": {
                    "key": "value"
                }
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(conciseJsonProp)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should add LanguageProperty type when languageMap key is present`() {
        val conciseLangProp = mapOf("languageMap" to mapOf("fr" to "Tour Eiffel", "en" to "Eiffel Tower"))

        val expectedNormalizedRepresentation = """
            {
                "type": "LanguageProperty",
                "languageMap": {
                    "fr": "Tour Eiffel",
                    "en": "Eiffel Tower"
                }
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(conciseLangProp)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should add VocabProperty type when vocab key is present`() {
        val conciseVocabProp = mapOf("vocab" to "Open")

        val expectedNormalizedRepresentation = """
            {
                "type": "VocabProperty",
                "vocab": "Open"
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(conciseVocabProp)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should add GeoProperty type when value is a GeoJSON map`() {
        val geoJson = mapOf(
            "type" to "Polygon",
            "coordinates" to listOf(listOf(listOf(100.0, 0.0), listOf(101.0, 1.0)))
        )
        val conciseGeo = mapOf("value" to geoJson, "observedAt" to "2024-01-01T00:00:00Z")

        val expectedNormalizedRepresentation = """
            {
                "type": "GeoProperty",
                "value": {
                    "type": "Polygon",
                    "coordinates": [[[100.0, 0.0], [101.0, 1.0]]]
                },
                "observedAt": "2024-01-01T00:00:00Z"
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(conciseGeo)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should add Property type when value is a non-GeoJSON map`() {
        val conciseProp = mapOf("value" to 55.0, "observedAt" to "2024-01-01T00:00:00Z")

        val expectedNormalizedRepresentation = """
            {
                "type": "Property",
                "value": 55.0,
                "observedAt": "2024-01-01T00:00:00Z"
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(conciseProp)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should wrap a map with no identifying key as a Property value`() {
        val unknownMap = mapOf("someKey" to "someValue", "anotherKey" to 42)

        val expectedNormalizedRepresentation = """
            {
                "type": "Property",
                "value": {
                    "someKey": "someValue",
                    "anotherKey": 42
                }
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(unknownMap)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should pass through an already-normalised Property unchanged`() {
        val normalised = mapOf(
            "type" to "Property",
            "value" to "someValue"
        )

        val expectedNormalizedRepresentation = """
            {
                "type": "Property",
                "value": "someValue"
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(normalised)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should pass through an already-normalised Relationship unchanged`() {
        val normalised = mapOf(
            "type" to "Relationship",
            "object" to "urn:ngsi-ld:Entity:01"
        )

        val expectedNormalizedRepresentation = """
            {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Entity:01"
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(normalised)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeAttributeFragment should recurse into sub-attributes of a Relationship`() {
        val conciseRel = mapOf(
            "object" to "urn:ngsi-ld:Entity:01",
            "observedAt" to "2024-01-01T00:00:00Z",
            "subProp" to mapOf("value" to "hello")
        )

        val expectedNormalizedRepresentation = """
            {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Entity:01",
                "observedAt": "2024-01-01T00:00:00Z",
                "subProp": {
                    "type": "Property",
                    "value": "hello"
                }
            }
        """.trimIndent()

        val result = normalizeAttributeFragment(conciseRel)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeEntityFragment should normalise each element of a List`() {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:E:01",
            "type" to "T",
            "rel" to listOf(
                mapOf("object" to "urn:ngsi-ld:Entity:01"),
                mapOf("object" to "urn:ngsi-ld:Entity:02")
            )
        )

        val expectedNormalizedRepresentation = """
            {
                "id": "urn:ngsi-ld:E:01",
                "type": "T",
                "rel": [
                    {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:Entity:01"
                    },
                    {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:Entity:02"
                    }
                ]
            }
        """.trimIndent()

        val result = normalizeEntityFragment(payload)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeEntityFragment should leave id, type, scope, and context untouched`() {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Entity:01",
            "type" to "MyType",
            "scope" to "/A",
            "@context" to "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
        )

        val expectedNormalizedRepresentation = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "MyType",
                "scope": "/A",
                "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            }
        """.trimIndent()

        val result = normalizeEntityFragment(payload)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeEntityFragment should wrap a non-core scalar attribute value as a Property`() {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Entity:01",
            "type" to "MyType",
            "temperature" to 22.5
        )

        val expectedNormalizedRepresentation = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "MyType",
                "temperature": {
                    "type": "Property",
                    "value": 22.5
                }
            }
        """.trimIndent()

        val result = normalizeEntityFragment(payload)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeEntityFragment should wrap a non-core array attribute value as a Property`() {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Entity:01",
            "type" to "MyType",
            "temperature" to listOf(1, 2)
        )

        val expectedNormalizedRepresentation = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "MyType",
                "temperature": {
                    "type": "Property",
                    "value": [1, 2]
                }
            }
        """.trimIndent()

        val result = normalizeEntityFragment(payload)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeEntityFragment should wrap a non-core object attribute value as a Property`() {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Entity:01",
            "type" to "MyType",
            "temperature" to mapOf("a" to 1, "b" to 2)
        )

        val expectedNormalizedRepresentation = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "MyType",
                "temperature": {
                    "type": "Property",
                    "value": {
                        "a": 1,
                        "b": 2
                    }
                }
            }
        """.trimIndent()

        val result = normalizeEntityFragment(payload)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeEntityFragment should wrap a non-core attribute map (with value key) as a Property`() {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Entity:01",
            "type" to "MyType",
            "temperature" to mapOf("value" to 22.5)
        )

        val expectedNormalizedRepresentation = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "MyType",
                "temperature": {
                    "type": "Property",
                    "value": 22.5
                }
            }
        """.trimIndent()

        val result = normalizeEntityFragment(payload)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeEntityFragment should add type to a Relationship attribute map`() {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Entity:01",
            "type" to "MyType",
            "isLinkedTo" to mapOf("object" to "urn:ngsi-ld:Entity:99")
        )

        val expectedNormalizedRepresentation = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "MyType",
                "isLinkedTo": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Entity:99"
                }
            }
        """.trimIndent()

        val result = normalizeEntityFragment(payload)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }

    @Test
    fun `normalizeEntityFragment should normalise each element of a multi-instance attribute`() {
        val payload = mapOf(
            "id" to "urn:ngsi-ld:Entity:01",
            "type" to "MyType",
            "speed" to listOf(
                mapOf("value" to 55, "datasetId" to "urn:ngsi-ld:Dataset:01"),
                mapOf("value" to 60, "datasetId" to "urn:ngsi-ld:Dataset:02")
            )
        )

        val expectedNormalizedRepresentation = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "MyType",
                "speed": [
                    {
                        "type": "Property",
                        "value": 55,
                        "datasetId": "urn:ngsi-ld:Dataset:01"
                    },
                    {
                        "type": "Property",
                        "value": 60,
                        "datasetId": "urn:ngsi-ld:Dataset:02"
                    }
                ]
            }
        """.trimIndent()

        val result = normalizeEntityFragment(payload)

        assertJsonPayloadsAreEqual(expectedNormalizedRepresentation, serializeObject(result))
    }
}
