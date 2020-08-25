package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NgsiLdGeoProperty
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_POINT_PROPERTY
import com.egm.stellio.subscription.model.GeoQuery
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory

object QueryUtils {

    const val NEAR_QUERY_CLAUSE = "near"
    const val DISTANCE_QUERY_CLAUSE = "distance"
    const val MAX_DISTANCE_QUERY_CLAUSE = "maxDistance"
    const val MIN_DISTANCE_QUERY_CLAUSE = "minDistance"
    const val PROPERTY_TYPE = "\"Property\""
    const val RELATIONSHIP_TYPE = "\"Relationship\""

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * This method transforms a subscription query as per clause 4.9 to new query format supported by JsonPath.
     * The query param is subscription related query to be transformed.
     * the entity param is the used to define the query attributes types (Property, Relationship, other) wich is used to extract the value to be compared as mandated by clause 4.9.

     * Examples of transformations:
     * foodQuantity>=150 -> @.foodQuantity.value>=150
     * foodQuantity>150;executes.createdAt=="2018-11-26T21:32:52.98601Z" -> @.foodQuantity.value>150&&@.executes.createdAt=="2018-11-26T21:32:52.98601Z"
     * (executes=="urn:ngsi-ld:Feeder:018z5"|executes[createdAt]=="2018-11-26T21:32:52.98601Z)" -> (@.executes.object=="urn:ngsi-ld:Feeder:018z5"||@.executes["createdAt"]=="2018-11-26T21:32:52.98601Z")
     */

    fun createQueryStatement(query: String?, entity: String): String {
        val mapper = jacksonObjectMapper()
        val parsedEntity = mapper.readTree(entity) as ObjectNode

        var jsonPathQuery = query

        query!!.split(';', '|').forEach { predicate ->
            val predicateParams = predicate.split("==", "!=", ">", ">=", "<", "<=")
            val attribute = predicateParams[0]
                .replace("(", "")
                .replace(")", "")

            val jsonPathAttribute = if ((attribute.isCompoundAttribute())) {
                when (getAttributeType(attribute, parsedEntity, "[").toString()) {
                    PROPERTY_TYPE -> "@.".plus(attribute).plus("[value]").addQuotesToBrackets()
                    RELATIONSHIP_TYPE -> "@.".plus(attribute).plus("[object]").addQuotesToBrackets()
                    else
                    -> "@.".plus(attribute).addQuotesToBrackets()
                }
            } else {
                when (getAttributeType(attribute, parsedEntity, ".").toString()) {
                    PROPERTY_TYPE -> "@.".plus(attribute).plus(".value")
                    RELATIONSHIP_TYPE -> "@.".plus(attribute).plus(".object")
                    else
                    -> "@.".plus(attribute)
                }
            }

            val jsonPathPredicate = predicate.replace(attribute, jsonPathAttribute)

            jsonPathQuery = jsonPathQuery!!.replace(predicate, jsonPathPredicate)
        }

        return jsonPathQuery!!.replace(";", "&&").replace("|", "||")
    }

    fun String.isCompoundAttribute(): Boolean =
        this.contains("\\[.*?]".toRegex())

    fun String.addQuotesToBrackets(): String =
        this.replace("[", "['").replace("]", "']")

    fun createGeoQueryStatement(geoQuery: GeoQuery?, location: NgsiLdGeoProperty): String {
        val locationInstance = location.instances[0]
        val refGeometryStatement = createSqlGeometry(geoQuery!!.geometry.name, geoQuery.coordinates)
        val targetGeometryStatement =
            createSqlGeometry(locationInstance.geoPropertyType, locationInstance.coordinates.toString())
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

        return mapper.readValue(
            coordinates.toString(),
            mapper.typeFactory.constructCollectionType(List::class.java, Any::class.java)
        )
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

    fun getAttributeType(attribute: String, entity: ObjectNode, separator: String): JsonNode? {
        var node: JsonNode = entity
        val attributePath = if (separator == "[")
            attribute.replace("]", "").split(separator)
        else
            attribute.split(separator)

        attributePath.forEach {
            try {
                node = node.get(it)
            } catch (e: Exception) {
                throw BadRequestDataException("Unmatched query since it contains an unknown attribute $it")
            }
        }

        return node.get("type")
    }
}
