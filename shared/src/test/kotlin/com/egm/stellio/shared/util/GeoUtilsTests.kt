package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test")
class GeoUtilsTests {

    @Test
    fun `geoJsonToWkt should preserve the altitude of a Point`() {
        val geoJson = """{ "type": "Point", "coordinates": [21.7, 38.2, 110.5] }"""

        assertEquals("POINT Z(21.7 38.2 110.5)", geoJsonToWkt(geoJson).shouldSucceedAndResult())
    }

    @Test
    fun `geoJsonToWkt should not add an altitude to a 2D Point`() {
        val geoJson = """{ "type": "Point", "coordinates": [21.7, 38.2] }"""

        assertEquals("POINT (21.7 38.2)", geoJsonToWkt(geoJson).shouldSucceedAndResult())
    }

    @Test
    fun `geoJsonToWkt should produce WKT that wktToGeoJson can parse back`() {
        val geoJson = """{ "type": "Point", "coordinates": [21.7, 38.2, 110.5] }"""

        val wkt = geoJsonToWkt(geoJson).shouldSucceedAndResult()

        assertEquals(deserializeObject(geoJson), wktToGeoJson(wkt))
    }

    @Test
    fun `geoJsonToWkt should preserve the altitude of a LineString`() {
        val geoJson =
            """{ "type": "LineString", "coordinates": [[21.7, 38.2, 110.5], [21.8, 38.3, 111.5]] }"""

        assertEquals(
            "LINESTRING Z(21.7 38.2 110.5, 21.8 38.3 111.5)",
            geoJsonToWkt(geoJson).shouldSucceedAndResult()
        )
    }
}
