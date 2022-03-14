package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.parseLocationFragmentToPointGeoProperty
import com.egm.stellio.subscription.model.GeoQuery
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [QueryUtils::class])
@ActiveProfiles("test")
class QueryUtilsTests {

    @Test
    fun `it should create a disjoints geoquery statement`() {
        val geoQuery = mockk<GeoQuery>()
        every { geoQuery.georel } returns "disjoint"
        every { geoQuery.pgisGeometry } returns "001"
        val ngsiLdGeoProperty = parseLocationFragmentToPointGeoProperty(24.30623, 60.07966)

        val queryStatement = QueryUtils.createGeoQueryStatement(geoQuery, ngsiLdGeoProperty)

        assertTrue(
            queryStatement.matchContent(
                """
                SELECT ST_disjoint('001', ST_GeomFromText('POINT (24.30623 60.07966)')) as match
                """
            )
        )
    }

    @Test
    fun `it should create a maxDistance geoquery statement`() {
        val geoQuery = mockk<GeoQuery>()
        every { geoQuery.georel } returns "near;maxDistance==2000"
        every { geoQuery.pgisGeometry } returns "001"
        val ngsiLdGeoProperty = parseLocationFragmentToPointGeoProperty(60.07966, 24.30623)

        val queryStatement = QueryUtils.createGeoQueryStatement(geoQuery, ngsiLdGeoProperty)

        assertTrue(
            queryStatement.matchContent(
                """
                SELECT ST_Distance('001'::geography, 'SRID=4326;POINT (60.07966 24.30623)'::geography) <= 2000 as match
                """
            )
        )
    }

    @Test
    fun `it should create a minDistance geoquery statement`() {
        val geoQuery = mockk<GeoQuery>()
        every { geoQuery.georel } returns "near;minDistance==15"
        every { geoQuery.pgisGeometry } returns "001"
        val ngsiLdGeoProperty = parseLocationFragmentToPointGeoProperty(60.30623, 30.07966)

        val queryStatement = QueryUtils.createGeoQueryStatement(geoQuery, ngsiLdGeoProperty)

        assertTrue(
            queryStatement.matchContent(
                """
                SELECT ST_Distance('001'::geography, 'SRID=4326;POINT (60.30623 30.07966)'::geography) >= 15 as match 
                """
            )
        )
    }
}
