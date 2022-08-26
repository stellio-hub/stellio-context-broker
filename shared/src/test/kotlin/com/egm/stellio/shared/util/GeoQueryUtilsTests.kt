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
    fun `it should correctly extract georel of a geo query with egal distance`() {
        val requestParams = gimmeFullParamsMap(georel = "near;egalDistance==1500")
        val geoQueryParams = parseAndCheckGeoQuery(requestParams, JsonLdUtils.NGSILD_CORE_CONTEXT)
        val georelParams = geoQueryParams.georel?.let { extractGeorelParams(it) }
        val georel = Triple(GeoQueryUtils.DISTANCE_QUERY_CLAUSE, "==", "1500")

        Assertions.assertEquals(georel, georelParams)
    }

    @Test
    fun `it should not extract near param of georel if it not present`() {
        val requestParams = gimmeFullParamsMap(georel = "distant")
        val geoQueryParams = parseAndCheckGeoQuery(requestParams, JsonLdUtils.NGSILD_CORE_CONTEXT)
        val georelParams = geoQueryParams.georel?.let { extractGeorelParams(it) }
        val georel = Triple(geoQueryParams.georel, null, null)

        Assertions.assertEquals(georel, georelParams)
    }

    @Test
    fun `function verifGeoQuery should return true`() {
        val requestParams = gimmeFullParamsMap()
        val geoQueryParams = parseAndCheckGeoQuery(requestParams, JsonLdUtils.NGSILD_CORE_CONTEXT)

        Assertions.assertTrue(verifGeoQuery(geoQueryParams))
    }

    @Test
    fun `function verifGeoQuery should return false`() {
        val geoQueryWithoutGeorel = GeoQuery()

        Assertions.assertFalse(verifGeoQuery(geoQueryWithoutGeorel))

        val geoQueryWithOperationSpace = GeoQuery(geoproperty = NGSILD_OPERATION_SPACE_PROPERTY)

        Assertions.assertFalse(verifGeoQuery(geoQueryWithOperationSpace))

        val geoQueryWithPolygon = GeoQuery(
            georel = "distant",
            geometry = "POLYGON ((7.49 43.78, 7.5 43.78, 7.5 43.79, 7.49 43.79, 7.49 43.78))"
        )

        Assertions.assertFalse(verifGeoQuery(geoQueryWithPolygon))

        val geoQueryWithoutCoordinates = GeoQuery(
            georel = "distant",
            geometry = "POINT (1.1 5.4)"
        )

        Assertions.assertFalse(verifGeoQuery(geoQueryWithoutCoordinates))
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
