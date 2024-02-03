package com.egm.stellio.search.util

import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXTS
import com.egm.stellio.shared.util.ngsiLdDateTime
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.time.LocalTime

@ActiveProfiles("test")
class AttributeInstanceUtilsTests {

    @Test
    fun `it should guess the value type of a string property`() = runTest {
        val expandedStringProperty = expandAttribute(
            "property",
            mapOf("type" to "Property", "value" to "A string"),
            NGSILD_TEST_CORE_CONTEXTS
        )
        assertEquals(
            TemporalEntityAttribute.AttributeValueType.STRING,
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Property, expandedStringProperty.second[0])
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
            TemporalEntityAttribute.AttributeValueType.NUMBER,
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Property, expandedBooleanProperty.second[0])
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
            TemporalEntityAttribute.AttributeValueType.NUMBER,
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Property, expandedBooleanProperty.second[0])
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
            TemporalEntityAttribute.AttributeValueType.BOOLEAN,
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Property, expandedBooleanProperty.second[0])
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
            TemporalEntityAttribute.AttributeValueType.OBJECT,
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Property, expandedListProperty.second[0])
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
            TemporalEntityAttribute.AttributeValueType.ARRAY,
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Property, expandedListProperty.second[0])
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
            TemporalEntityAttribute.AttributeValueType.TIME,
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Property, expandedTimeProperty.second[0])
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
            TemporalEntityAttribute.AttributeValueType.DATETIME,
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Property, expandedTimeProperty.second[0])
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
            TemporalEntityAttribute.AttributeValueType.GEOMETRY,
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.GeoProperty, expandedGeoProperty.second[0])
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
            TemporalEntityAttribute.AttributeValueType.JSON,
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.JsonProperty, expandedJsonProperty.second[0])
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
            TemporalEntityAttribute.AttributeValueType.URI,
            guessAttributeValueType(
                TemporalEntityAttribute.AttributeType.Relationship,
                expandedGeoRelationship.second[0]
            )
        )
    }
}
