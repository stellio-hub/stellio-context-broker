package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.GeoQuery
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OPERATION_SPACE_PROPERTY
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap

@ActiveProfiles("test")
class GeoQueryUtilsTests {

    @Test
    fun `it should parse geo query parameters`() {
        val requestParams = gimmeFullParamsMap()
        val geoQueryParams = parseAndCheckGeoQuery(requestParams, JsonLdUtils.NGSILD_CORE_CONTEXT)

        val geoQuery = GeoQuery(
            georel = "near;maxDistance==1500",
            geometry = "Point",
            coordinates = "[57.5522, -20.3484]",
            geoproperty = NGSILD_LOCATION_PROPERTY
        )
        Assertions.assertEquals(geoQuery, geoQueryParams)
    }

    @Test
    fun `it should parse geo query parameters with geoproperty operation space`() {
        val requestParams = gimmeFullParamsMap("operationSpace")

        val geoQueryParams = parseAndCheckGeoQuery(requestParams, JsonLdUtils.NGSILD_CORE_CONTEXT)

        val geoQuery = GeoQuery(
            georel = "near;maxDistance==1500",
            geometry = "Point",
            coordinates = "[57.5522, -20.3484]",
            geoproperty = NGSILD_OPERATION_SPACE_PROPERTY
        )
        Assertions.assertEquals(geoQuery, geoQueryParams)
    }

    @Test
    fun `it should correctly extract georel of a geo query`() {
        val requestParams = gimmeFullParamsMap()
        val geoQueryParams = parseAndCheckGeoQuery(requestParams, JsonLdUtils.NGSILD_CORE_CONTEXT)
        val georelParams = geoQueryParams.georel?.let { extractGeorelParams(it) }
        val georel = Triple(GeoQueryUtils.DISTANCE_QUERY_CLAUSE, "<=", "1500")

        Assertions.assertEquals(georel, georelParams)
    }

    @Test
    fun `it should correctly extract georel of a geo query with min distance`() {
        val requestParams = gimmeFullParamsMap(georel = "near;minDistance==1500")
        val geoQueryParams = parseAndCheckGeoQuery(requestParams, JsonLdUtils.NGSILD_CORE_CONTEXT)
        val georelParams = geoQueryParams.georel?.let { extractGeorelParams(it) }
        val georel = Triple(GeoQueryUtils.DISTANCE_QUERY_CLAUSE, ">=", "1500")

        Assertions.assertEquals(georel, georelParams)
    }

    @Test
    fun `it should not extract near param of georel if it is not present`() {
        val requestParams = gimmeFullParamsMap(georel = "distant")
        val geoQueryParams = parseAndCheckGeoQuery(requestParams, JsonLdUtils.NGSILD_CORE_CONTEXT)
        val georelParams = geoQueryParams.georel?.let { extractGeorelParams(it) }
        val georel = Triple(geoQueryParams.georel, null, null)

        Assertions.assertEquals(georel, georelParams)
    }

    @Test
    fun `function isSupportedGeoQuery should return true`() {
        val requestParams = gimmeFullParamsMap()
        val geoQueryParams = parseAndCheckGeoQuery(requestParams, JsonLdUtils.NGSILD_CORE_CONTEXT)

        Assertions.assertTrue(isSupportedGeoQuery(geoQueryParams))
    }

    @Test
    fun `function isSupportedGeoQuery should return false`() {
        val geoQueryWithoutGeorel = GeoQuery()

        Assertions.assertFalse(isSupportedGeoQuery(geoQueryWithoutGeorel))

        val geoQueryWithOperationSpace = GeoQuery(geoproperty = NGSILD_OPERATION_SPACE_PROPERTY)

        Assertions.assertFalse(isSupportedGeoQuery(geoQueryWithOperationSpace))

        val geoQueryWithPolygon = GeoQuery(
            georel = "distant",
            geometry = "Polygon"
        )

        Assertions.assertFalse(isSupportedGeoQuery(geoQueryWithPolygon))

        val geoQueryWithoutCoordinates = GeoQuery(
            georel = "distant",
            geometry = "Point"
        )

        Assertions.assertFalse(isSupportedGeoQuery(geoQueryWithoutCoordinates))
    }

    private fun gimmeFullParamsMap(
        geoproperty: String? = "location",
        georel: String? = "near;maxDistance==1500"
    ): LinkedMultiValueMap<String, String> {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("georel", georel)
        requestParams.add("geometry", "Point")
        requestParams.add("coordinates", "[57.5522, -20.3484]")
        requestParams.add("geoproperty", geoproperty)
        return requestParams
    }
}
