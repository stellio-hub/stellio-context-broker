package com.egm.stellio.search.util

import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.WKTCoordinates
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.time.LocalTime

@ActiveProfiles("test")
class AttributeInstanceUtilsTests {

    @Test
    fun `it should guess the value type of a property`() {
        assertEquals(
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Property, "A string"),
            TemporalEntityAttribute.AttributeValueType.STRING
        )
        assertEquals(
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Property, LocalTime.now()),
            TemporalEntityAttribute.AttributeValueType.TIME
        )
    }

    @Test
    fun `it should guess the value type of a geo-property`() {
        assertEquals(
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.GeoProperty, WKTCoordinates("0 0")),
            TemporalEntityAttribute.AttributeValueType.GEOMETRY
        )
    }

    @Test
    fun `it should guess the value type of a relationship`() {
        assertEquals(
            guessAttributeValueType(TemporalEntityAttribute.AttributeType.Relationship, URI("urn:ngsi-ld")),
            TemporalEntityAttribute.AttributeValueType.URI
        )
    }
}
