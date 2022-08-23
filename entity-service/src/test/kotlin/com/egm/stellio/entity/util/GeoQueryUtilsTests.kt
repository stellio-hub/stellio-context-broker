package com.egm.stellio.entity.util

import com.egm.stellio.entity.model.GeoQuery
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
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
    fun `extract georel params`() {
        val requestParams = gimmeFullParamsMap()
        val geoQueryParams = parseAndCheckGeoQuery(requestParams, JsonLdUtils.NGSILD_CORE_CONTEXT)
        val georelParams = geoQueryParams.georel?.let { extractGeorelParams(it) }
        val georel = Triple(GeoQueryUtils.DISTANCE_QUERY_CLAUSE, "<=", "1500")

        Assertions.assertEquals(georel, georelParams)
    }

    private fun gimmeFullParamsMap(): LinkedMultiValueMap<String, String> {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("georel", "near;maxDistance==1500")
        requestParams.add("geometry", "Point")
        requestParams.add("coordinates", "[57.5522, -20.3484]")
        requestParams.add("geoproperty", "location")
        return requestParams
    }
}
