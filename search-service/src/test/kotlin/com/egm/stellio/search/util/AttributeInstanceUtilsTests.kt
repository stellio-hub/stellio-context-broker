package com.egm.stellio.search.util

import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.util.DEFAULT_CONTEXTS
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.time.LocalTime

@OptIn(ExperimentalCoroutinesApi::class)
@ActiveProfiles("test")
class AttributeInstanceUtilsTests {

    @Test
    fun `it should guess the value type of a property`() = runTest {
        val expandedStringProperty = expandAttribute(
            "property",
            mapOf("type" to "Property", "value" to "A string"),
            DEFAULT_CONTEXTS
        )
        assertEquals(
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Property, expandedStringProperty.second[0]),
            TemporalEntityAttribute.AttributeValueType.STRING
        )

        val expandedTimeProperty = expandAttribute(
            "property",
            mapOf("type" to "Property", "value" to LocalTime.now()),
            DEFAULT_CONTEXTS
        )
        assertEquals(
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Property, expandedTimeProperty.second[0]),
            TemporalEntityAttribute.AttributeValueType.TIME
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
            DEFAULT_CONTEXTS
        )
        assertEquals(
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.GeoProperty, expandedGeoProperty.second[0]),
            TemporalEntityAttribute.AttributeValueType.GEOMETRY
        )
    }

    @Test
    fun `it should guess the value type of a relationship`() = runTest {
        val expandedGeoRelationship = expandAttribute(
            "relationship",
            mapOf("type" to "Relationship", "value" to URI("urn:ngsi-ld")),
            DEFAULT_CONTEXTS
        )
        assertEquals(
            guessAttributeValueType(
                TemporalEntityAttribute.AttributeType.Relationship,
                expandedGeoRelationship.second[0]
            ),
            TemporalEntityAttribute.AttributeValueType.URI
        )
    }
}
