package com.egm.stellio.search.entity.util

import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.Attribute.AttributeType
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXTS
import com.egm.stellio.shared.util.ngsiLdDateTime
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.time.LocalTime

@ActiveProfiles("test")
class AttributeUtilsTests {

    @Test
    fun `it should guess the value type of a string property`() = runTest {
        val expandedStringProperty = expandAttribute(
            "property",
            mapOf("type" to "Property", "value" to "A string"),
            NGSILD_TEST_CORE_CONTEXTS
        )
        assertEquals(
            Attribute.AttributeValueType.STRING,
            guessAttributeValueType(AttributeType.Property, expandedStringProperty.second[0])
        )
    }

    @Test
    fun `it should guess the value type of a double property`() = runTest {
        val expandedBooleanProperty = expandAttribute(
            "property",
            mapOf("type" to "Property", "value" to 20.0),
            NGSILD_TEST_CORE_CONTEXTS
        )
        assertEquals(
            Attribute.AttributeValueType.NUMBER,
            guessAttributeValueType(AttributeType.Property, expandedBooleanProperty.second[0])
        )
    }

    @Test
    fun `it should guess the value type of an int property`() = runTest {
        val expandedBooleanProperty = expandAttribute(
            "property",
            mapOf("type" to "Property", "value" to 20),
            NGSILD_TEST_CORE_CONTEXTS
        )
        assertEquals(
            Attribute.AttributeValueType.NUMBER,
            guessAttributeValueType(AttributeType.Property, expandedBooleanProperty.second[0])
        )
    }

    @Test
    fun `it should guess the value type of a boolean property`() = runTest {
        val expandedBooleanProperty = expandAttribute(
            "property",
            mapOf("type" to "Property", "value" to true),
            NGSILD_TEST_CORE_CONTEXTS
        )
        assertEquals(
            Attribute.AttributeValueType.BOOLEAN,
            guessAttributeValueType(AttributeType.Property, expandedBooleanProperty.second[0])
        )
    }

    @Test
    fun `it should guess the value type of an object property`() = runTest {
        val expandedListProperty = expandAttribute(
            "property",
            mapOf("type" to "Property", "value" to mapOf("key1" to "value1", "key2" to "value3")),
            NGSILD_TEST_CORE_CONTEXTS
        )
        assertEquals(
            Attribute.AttributeValueType.OBJECT,
            guessAttributeValueType(AttributeType.Property, expandedListProperty.second[0])
        )
    }

    @Test
    fun `it should guess the value type of an array property`() = runTest {
        val expandedListProperty = expandAttribute(
            "property",
            mapOf("type" to "Property", "value" to listOf("A", "B")),
            NGSILD_TEST_CORE_CONTEXTS
        )
        assertEquals(
            Attribute.AttributeValueType.ARRAY,
            guessAttributeValueType(AttributeType.Property, expandedListProperty.second[0])
        )
    }

    @Test
    fun `it should guess the value type of a time property`() = runTest {
        val expandedTimeProperty = expandAttribute(
            "property",
            mapOf("type" to "Property", "value" to mapOf("@type" to "Time", "@value" to LocalTime.now())),
            NGSILD_TEST_CORE_CONTEXTS
        )
        assertEquals(
            Attribute.AttributeValueType.TIME,
            guessAttributeValueType(AttributeType.Property, expandedTimeProperty.second[0])
        )
    }

    @Test
    fun `it should guess the value type of a datetime property`() = runTest {
        val expandedTimeProperty = expandAttribute(
            "property",
            mapOf("type" to "Property", "value" to mapOf("@type" to "DateTime", "@value" to ngsiLdDateTime())),
            NGSILD_TEST_CORE_CONTEXTS
        )
        assertEquals(
            Attribute.AttributeValueType.DATETIME,
            guessAttributeValueType(AttributeType.Property, expandedTimeProperty.second[0])
        )
    }

    @Test
    fun `it should guess the value type of a geo-property`() = runTest {
        val expandedGeoProperty = expandAttribute(
            "location",
            mapOf(
                "type" to "GeoProperty",
                "value" to mapOf("type" to "Point", "coordinates" to listOf(0, 0))
            ),
            NGSILD_TEST_CORE_CONTEXTS
        )
        assertEquals(
            Attribute.AttributeValueType.GEOMETRY,
            guessAttributeValueType(AttributeType.GeoProperty, expandedGeoProperty.second[0])
        )
    }

    @Test
    fun `it should guess the value type of a JSON property`() = runTest {
        val expandedJsonProperty = expandAttribute(
            "jsonProperty",
            mapOf(
                "type" to "JsonProperty",
                "json" to mapOf("key" to "value")
            ),
            NGSILD_TEST_CORE_CONTEXTS
        )
        assertEquals(
            Attribute.AttributeValueType.JSON,
            guessAttributeValueType(AttributeType.JsonProperty, expandedJsonProperty.second[0])
        )
    }

    @Test
    fun `it should guess the value type of a relationship`() = runTest {
        val expandedGeoRelationship = expandAttribute(
            "relationship",
            mapOf("type" to "Relationship", "value" to URI("urn:ngsi-ld")),
            NGSILD_TEST_CORE_CONTEXTS
        )
        assertEquals(
            Attribute.AttributeValueType.URI,
            guessAttributeValueType(
                AttributeType.Relationship,
                expandedGeoRelationship.second[0]
            )
        )
    }

    @Test
    fun `it should find a Property whose value is NGSI-LD Null`() = runTest {
        val expandedProperty = expandAttribute(
            """
                {
                    "property": {
                        "type": "Property",
                        "value": "urn:ngsi-ld:null"
                    }
                }
            """.trimIndent(),
            NGSILD_TEST_CORE_CONTEXTS
        ).second[0]

        assertTrue(hasNgsiLdNullValue(expandedProperty, AttributeType.Property))
    }

    @Test
    fun `it should find a LanguageProperty whose value is NGSI-LD Null`() = runTest {
        val expandedProperty = expandAttribute(
            """
                {
                    "langProperty": {
                        "type": "LanguageProperty",
                        "languageMap": {
                            "@none": "urn:ngsi-ld:null"
                        }
                    }
                }
            """.trimIndent(),
            NGSILD_TEST_CORE_CONTEXTS
        ).second[0]

        assertTrue(hasNgsiLdNullValue(expandedProperty, AttributeType.LanguageProperty))
    }

    @Test
    fun `it should find a JsonProperty whose value is NGSI-LD Null`() = runTest {
        val expandedProperty = expandAttribute(
            """
                {
                    "jsonProperty": {
                        "type": "JsonProperty",
                        "json": "urn:ngsi-ld:null"
                    }
                }
            """.trimIndent(),
            NGSILD_TEST_CORE_CONTEXTS
        ).second[0]

        assertTrue(hasNgsiLdNullValue(expandedProperty, AttributeType.JsonProperty))
    }

    @Test
    fun `it should find a Relationship whose value is NGSI-LD Null`() = runTest {
        val expandedProperty = expandAttribute(
            """
                {
                    "relationship": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:null"
                    }
                }
            """.trimIndent(),
            NGSILD_TEST_CORE_CONTEXTS
        ).second[0]

        assertTrue(hasNgsiLdNullValue(expandedProperty, AttributeType.Relationship))
    }
}
