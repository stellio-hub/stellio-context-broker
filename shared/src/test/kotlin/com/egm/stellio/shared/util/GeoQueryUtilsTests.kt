package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.param.GeoQuery
import com.egm.stellio.shared.model.param.GeoQuery.GeometryType
import com.egm.stellio.shared.model.param.buildGeoQuery
import com.egm.stellio.shared.model.param.parseGeoQueryParameters
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OPERATION_SPACE_PROPERTY
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test")
class GeoQueryUtilsTests {

    @Test
    fun `it should parse geo query parameters`() = runTest {
        val requestParams = gimmeFullParamsMap()
        val geoQueryParams =
            parseGeoQueryParameters(requestParams, NGSILD_TEST_CORE_CONTEXTS).shouldSucceedAndResult()

        val geoQuery = GeoQuery(
            georel = "near;maxDistance==1500",
            geometry = GeometryType.POINT,
            coordinates = "[57.5522, -20.3484]",
            wktCoordinates = geoJsonToWkt(GeometryType.POINT, "[57.5522, -20.3484]").getOrNull()!!,
            geoproperty = NGSILD_LOCATION_PROPERTY
        )
        assertEquals(geoQuery, geoQueryParams)
    }

    @Test
    fun `it should parse URL encoded geo query parameters`() = runTest {
        val requestParams = gimmeFullParamsMap(
            georel = "near%3BmaxDistance%3D%3D1500"
        ).plus("coordinates" to "[57.5522%2C%20-20.3484]")
        val geoQueryParams =
            parseGeoQueryParameters(requestParams, NGSILD_TEST_CORE_CONTEXTS).shouldSucceedAndResult()

        val geoQuery = GeoQuery(
            georel = "near;maxDistance==1500",
            geometry = GeometryType.POINT,
            coordinates = "[57.5522, -20.3484]",
            wktCoordinates = geoJsonToWkt(GeometryType.POINT, "[57.5522, -20.3484]").getOrNull()!!,
            geoproperty = NGSILD_LOCATION_PROPERTY
        )
        assertEquals(geoQuery, geoQueryParams)
    }

    @Test
    fun `it should parse geo query parameters with geoproperty operation space`() = runTest {
        val requestParams = gimmeFullParamsMap("operationSpace")

        val geoQueryParams =
            parseGeoQueryParameters(requestParams, NGSILD_TEST_CORE_CONTEXTS).shouldSucceedAndResult()

        val geoQuery = GeoQuery(
            georel = "near;maxDistance==1500",
            geometry = GeometryType.POINT,
            coordinates = "[57.5522, -20.3484]",
            wktCoordinates = geoJsonToWkt(GeometryType.POINT, "[57.5522, -20.3484]").getOrNull()!!,
            geoproperty = NGSILD_OPERATION_SPACE_PROPERTY
        )
        assertEquals(geoQuery, geoQueryParams)
    }

    @Test
    fun `it should fail to create a geoquery if georel has an invalid near clause`() = runTest {
        val requestParams = gimmeFullParamsMap(georel = "near;distance<100")
        parseGeoQueryParameters(requestParams, NGSILD_TEST_CORE_CONTEXTS).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals("Invalid expression for 'near' georel: near;distance<100", it.message)
        }
    }

    @Test
    fun `it should fail to create a geoquery if georel is not recognized`() = runTest {
        val requestParams = gimmeFullParamsMap(georel = "unrecognized")
        parseGeoQueryParameters(requestParams, NGSILD_TEST_CORE_CONTEXTS).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals("Invalid 'georel' parameter provided: unrecognized", it.message)
        }
    }

    @Test
    fun `it should fail to create a geoquery if geometry is not recognized`() = runTest {
        val requestParams = gimmeFullParamsMap(geometry = "Unrecognized")
        parseGeoQueryParameters(requestParams, NGSILD_TEST_CORE_CONTEXTS).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals("Unrecognized is not a recognized value for 'geometry' parameter", it.message)
        }
    }

    @Test
    fun `it should fail to create a geoquery if a required parameter is missing`() = runTest {
        val geoQueryParameters = mapOf(
            "geometry" to "Point",
            "coordinates" to "[57.5522,%20-20.3484]"
        )
        parseGeoQueryParameters(geoQueryParameters, NGSILD_TEST_CORE_CONTEXTS).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Missing at least one geo parameter between 'geometry', 'georel' and 'coordinates'",
                it.message
            )
        }
    }

    @Test
    fun `it should fail to create a geoquery if coordinates are invalid`() = runTest {
        val geoQueryParameters = mapOf(
            "georel" to "within",
            "geometry" to "Polygon",
            "coordinates" to "[57.5522,%20-20.3484]"
        )
        parseGeoQueryParameters(geoQueryParameters, NGSILD_TEST_CORE_CONTEXTS).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEqualsIgnoringNoise(
                """
                Invalid geometry definition: {
                    "type": "Polygon",
                    "coordinates": [57.5522, -20.3484]
                } (org.locationtech.jts.io.ParseException: Could not parse Polygon from GeoJson string.)
                """.trimIndent(),
                it.message
            )
        }
    }

    @Test
    fun `it should create a disjoint geoquery statement`() = runTest {
        val geoQuery = GeoQuery(
            "disjoint",
            GeometryType.POLYGON,
            "[[[0 1],[1, 1],[0, 1]]]",
            geoJsonToWkt(GeometryType.POLYGON, "[[[0 1],[1, 1],[0, 1]]]").getOrNull()!!
        )
        val expandedEntity = gimmeSimpleEntityWithGeoProperty("location", 24.30623, 60.07966)

        val queryStatement = buildGeoQuery(geoQuery, expandedEntity)

        assertEqualsIgnoringNoise(
            """
            public.ST_disjoint(
                public.ST_GeomFromText('POLYGON ((0 1, 1 1, 0 1))'), 
                public.ST_GeomFromText((select jsonb_path_query_first('{"@id":"urn:ngsi-ld:Entity:01","@type":["https://uri.etsi.org/ngsi-ld/default-context/Entity"],"https://uri.etsi.org/ngsi-ld/location":[{"@type":["https://uri.etsi.org/ngsi-ld/GeoProperty"],"https://uri.etsi.org/ngsi-ld/hasValue":[{"@value":"POINT (24.30623 60.07966)"}]}]}',
                    '$."https://uri.etsi.org/ngsi-ld/location"."https://uri.etsi.org/ngsi-ld/hasValue"[0]')->>'@value'))
            )
            """,
            queryStatement
        )
    }

    @Test
    fun `it should create a maxDistance geoquery statement`() = runTest {
        val geoQuery = GeoQuery(
            "near;maxDistance==2000",
            GeometryType.POINT,
            "[60.10000, 24.60000]",
            geoJsonToWkt(GeometryType.POINT, "[60.10000, 24.60000]").getOrNull()!!
        )
        val expandedEntity = gimmeSimpleEntityWithGeoProperty("location", 60.07966, 24.30623)

        val queryStatement = buildGeoQuery(geoQuery, expandedEntity)

        assertEqualsIgnoringNoise(
            """
            public.ST_Distance(
                cast('SRID=4326;POINT(60.124.6)' as public.geography),
                cast('SRID=4326;' || (select jsonb_path_query_first('{"@id":"urn:ngsi-ld:Entity:01","@type":["https://uri.etsi.org/ngsi-ld/default-context/Entity"],"https://uri.etsi.org/ngsi-ld/location":[{"@type":["https://uri.etsi.org/ngsi-ld/GeoProperty"],"https://uri.etsi.org/ngsi-ld/hasValue":[{"@value":"POINT(60.0796624.30623)"}]}]}','$."https://uri.etsi.org/ngsi-ld/location"."https://uri.etsi.org/ngsi-ld/hasValue"[0]')->>'@value') as public.geography),
                false
            ) <= 2000
            """,
            queryStatement
        )
    }

    @Test
    fun `it should create a minDistance geoquery statement`() = runTest {
        val geoQuery = GeoQuery(
            "near;minDistance==15",
            GeometryType.POINT,
            "[60.10000, 24.60000]",
            geoJsonToWkt(GeometryType.POINT, "[60.10000, 24.60000]").getOrNull()!!
        )
        val expandedEntity = gimmeSimpleEntityWithGeoProperty("location", 60.30623, 30.07966)

        val queryStatement = buildGeoQuery(geoQuery, expandedEntity)

        assertEqualsIgnoringNoise(
            """
            public.ST_Distance(
                cast('SRID=4326;POINT(60.124.6)' as public.geography),
                cast('SRID=4326;' || (select jsonb_path_query_first('{"@id":"urn:ngsi-ld:Entity:01","@type":["https://uri.etsi.org/ngsi-ld/default-context/Entity"],"https://uri.etsi.org/ngsi-ld/location":[{"@type":["https://uri.etsi.org/ngsi-ld/GeoProperty"],"https://uri.etsi.org/ngsi-ld/hasValue":[{"@value":"POINT(60.3062330.07966)"}]}]}','$."https://uri.etsi.org/ngsi-ld/location"."https://uri.etsi.org/ngsi-ld/hasValue"[0]')->>'@value') as public.geography),
                false
            ) >= 15
            """,
            queryStatement
        )
    }

    private fun gimmeFullParamsMap(
        geoproperty: String = "location",
        georel: String = "near;maxDistance==1500",
        geometry: String = "Point"
    ): Map<String, String> = mapOf(
        "georel" to georel,
        "geometry" to geometry,
        "coordinates" to "[57.5522,%20-20.3484]",
        "geoproperty" to geoproperty
    )

    private suspend fun gimmeSimpleEntityWithGeoProperty(
        propertyKey: String,
        longitude: Double,
        latitude: Double
    ): ExpandedEntity {
        val entityWithLocation =
            """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "$propertyKey": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Point",
                        "coordinates": [
                            $longitude,
                            $latitude
                        ]
                    }
                }
            }
            """.trimIndent()

        return JsonLdUtils.expandJsonLdEntity(entityWithLocation, NGSILD_TEST_CORE_CONTEXTS)
    }
}
