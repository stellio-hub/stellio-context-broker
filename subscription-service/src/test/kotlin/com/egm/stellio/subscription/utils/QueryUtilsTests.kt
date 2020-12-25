package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.parseLocationFragmentToPointGeoProperty
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [QueryUtils::class])
@ActiveProfiles("test")
class QueryUtilsTests {

    @Test
    fun `it should create a disjoints geoquery statement`() {
        val geoQuery = gimmeRawSubscription(georel = "disjoint").geoQ
        val ngsiLdGeoProperty = parseLocationFragmentToPointGeoProperty(24.30623, 60.07966)

        val queryStatement = QueryUtils.createGeoQueryStatement(geoQuery, ngsiLdGeoProperty)

        assertTrue(
            queryStatement.matchContent(
                """
                SELECT ST_disjoint(
                    ST_GeomFromText('Polygon((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))'), 
                    ST_GeomFromText('Point(24.30623 60.07966)')
                ) as geoquery_result
                """
            )
        )
    }

    @Test
    fun `it should create a maxDistance geoquery statement`() {
        val geoQuery = gimmeRawSubscription(georel = "near;maxDistance==2000").geoQ
        val ngsiLdGeoProperty = parseLocationFragmentToPointGeoProperty(60.07966, 24.30623)

        val queryStatement = QueryUtils.createGeoQueryStatement(geoQuery, ngsiLdGeoProperty)

        assertTrue(
            queryStatement.matchContent(
                """
                SELECT ST_distance(ST_GeomFromText('Polygon((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))'),
                                   ST_GeomFromText('Point(60.07966 24.30623)')) <= 2000 as geoquery_result 
                """
            )
        )
    }

    @Test
    fun `it should create a minDistance geoquery statement`() {
        val geoQuery = gimmeRawSubscription(georel = "near;minDistance==15").geoQ
        val ngsiLdGeoProperty = parseLocationFragmentToPointGeoProperty(60.30623, 30.07966)

        val queryStatement = QueryUtils.createGeoQueryStatement(geoQuery, ngsiLdGeoProperty)

        assertTrue(
            queryStatement.matchContent(
                """
                SELECT ST_distance(ST_GeomFromText('Polygon((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))'),
                                   ST_GeomFromText('Point(60.30623 30.07966)')) >= 15 as geoquery_result 
                """
            )
        )
    }

    @Test
    fun `it should create an sql Polygon geometry`() {
        val geoQuery = gimmeRawSubscription().geoQ

        val queryStatement = QueryUtils.createSqlGeometry(geoQuery!!.geometry.name, geoQuery.coordinates.toString())

        assertEquals(
            queryStatement,
            "Polygon((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))"
        )
    }

    @Test
    fun `it should create an sql Point geometry`() {
        val targetGeometry = mapOf("geometry" to "Point", "coordinates" to "[60.30623, 30.07966]")

        val queryStatement = QueryUtils.createSqlGeometry(
            targetGeometry["geometry"].toString(),
            targetGeometry["coordinates"].toString()
        )

        assertEquals(
            queryStatement,
            "Point(60.30623 30.07966)"
        )
    }

    @Test
    fun `it should correctly parse Polygon coordinates`() {
        val geoQuery = gimmeRawSubscription().geoQ

        val parsedCoordinates = QueryUtils.parseCoordinates(geoQuery!!.geometry.name, geoQuery.coordinates.toString())

        assertEquals(
            parsedCoordinates,
            listOf(listOf(100.0, 0.0), listOf(101.0, 0.0), listOf(101.0, 1.0), listOf(100.0, 1.0), listOf(100.0, 0.0))
        )
    }

    @Test
    fun `it should correctly parse Point coordinates`() {
        val targetGeometry = mapOf("geometry" to "Point", "coordinates" to "[90.30623, 15.07966]")

        val parsedCoordinates =
            QueryUtils.parseCoordinates(targetGeometry["geometry"].toString(), targetGeometry["coordinates"].toString())

        assertEquals(
            parsedCoordinates,
            listOf(listOf(90.30623, 15.07966))
        )
    }
}
