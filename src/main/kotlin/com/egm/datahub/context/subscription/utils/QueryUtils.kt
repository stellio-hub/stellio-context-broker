package com.egm.datahub.context.subscription.utils

import com.egm.datahub.context.subscription.model.GeoQuery
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.egm.datahub.context.subscription.utils.NgsiLdParsingUtils.NGSILD_POINT_PROPERTY
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
object QueryUtils {

    const val NEAR_QUERY_CLAUSE = "near"
    const val DISTANCE_QUERY_CLAUSE = "distance"
    const val MAX_DISTANCE_QUERY_CLAUSE = "maxDistance"
    const val MIN_DISTANCE_QUERY_CLAUSE = "minDistance"

    private val logger = LoggerFactory.getLogger(javaClass)

    fun createGeoQueryStatement(geoQuery: GeoQuery?, targetGeometry: Map<String, Any>): String {
        val refGeometryStatement = createSqlGeometry(geoQuery!!.geometry.name, geoQuery.coordinates)
        val targetGeometryStatement = createSqlGeometry(targetGeometry.get("geometry").toString(), targetGeometry.get("coordinates").toString())
        val georelParams = extractGeorelParams(geoQuery.georel)

        if (georelParams.first == DISTANCE_QUERY_CLAUSE)
            return "SELECT ST_${georelParams.first}(ST_GeomFromText('$refGeometryStatement'), ST_GeomFromText('$targetGeometryStatement')) ${georelParams.second} ${georelParams.third} as geoquery_result"
        return "SELECT ST_${georelParams.first}(ST_GeomFromText('$refGeometryStatement'), ST_GeomFromText('$targetGeometryStatement')) as geoquery_result"
    }

    fun createSqlGeometry(geometryType: String, coordinates: String): String {
        val geometry = StringBuilder()
        val parsedCoordinates = parseCoordinates(geometryType, coordinates)

        if (geometryType == NGSILD_POINT_PROPERTY)
            geometry.append("$geometryType(")
        else
            geometry.append("$geometryType((")
        parsedCoordinates.forEach {
            geometry.append(it[0]).append(" ").append(it[1]).append(", ")
        }

        if (geometryType == NGSILD_POINT_PROPERTY)
            geometry.replace(geometry.length - 2, geometry.length, ")")
        else
            geometry.replace(geometry.length - 2, geometry.length, "))")

        return geometry.toString()
    }

    fun parseCoordinates(geometryType: String, initialCoordinates: String): List<List<Double>> {
        val mapper = jacksonObjectMapper()
        val coordinates = StringBuilder()
        if (geometryType == NGSILD_POINT_PROPERTY)
            coordinates.append("[").append(initialCoordinates).append("]")
        else
            coordinates.append(initialCoordinates)

        return mapper.readValue(coordinates.toString(), mapper.typeFactory.constructCollectionType(List::class.java, Any::class.java))
    }

    fun extractGeorelParams(georel: String): Triple<String, String?, String?> {
        if (georel.contains(NEAR_QUERY_CLAUSE)) {
            val comparaisonParams = georel.split(";")[1].split("==")
            if (comparaisonParams[0] == MAX_DISTANCE_QUERY_CLAUSE)
                return Triple(DISTANCE_QUERY_CLAUSE, "<=", comparaisonParams[1])
            return Triple(DISTANCE_QUERY_CLAUSE, ">=", comparaisonParams[1])
        }
        return Triple(georel, null, null)
    }
}