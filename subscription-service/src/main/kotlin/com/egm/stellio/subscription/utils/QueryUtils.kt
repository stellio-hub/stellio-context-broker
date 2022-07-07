package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NgsiLdGeoProperty
import com.egm.stellio.shared.util.mapper
import com.egm.stellio.subscription.model.GeoQuery
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

object QueryUtils {

    private const val NEAR_QUERY_CLAUSE = "near"
    private const val DISTANCE_QUERY_CLAUSE = "distance"
    private const val MAX_DISTANCE_QUERY_CLAUSE = "maxDistance"
    private const val MIN_DISTANCE_QUERY_CLAUSE = "minDistance"
    private const val PROPERTY_TYPE = "\"Property\""
    private const val RELATIONSHIP_TYPE = "\"Relationship\""

    /**
     * This method transforms a subscription query as per clause 4.9 to new query format supported by JsonPath.
     * The query param is subscription related query to be transformed.
     * the entity param is the used to define the query attributes types (Property, Relationship, other)
     * which is used to extract the value to be compared as mandated by clause 4.9.

     * Examples of transformations:
     * foodQuantity>=150 -> @.foodQuantity.value>=150
     * foodQuantity>150;executes.createdAt=="2018-11-26T21:32:52.98601Z" ->
     *     @.foodQuantity.value>150&&@.executes.createdAt=="2018-11-26T21:32:52.98601Z"
     * (executes=="urn:ngsi-ld:Feeder:018z5"|executes[createdAt]=="2018-11-26T21:32:52.98601Z)" ->
     *     (@.executes.object=="urn:ngsi-ld:Feeder:018z5"||@.executes["createdAt"]=="2018-11-26T21:32:52.98601Z")
     */
    fun createQueryStatement(query: String?, entity: String): String {
        val parsedEntity = mapper.readTree(entity) as ObjectNode

        var jsonPathQuery = query

        query!!.split(';', '|').forEach { predicate ->
            val predicateParams = predicate.split("==", "!=", ">", ">=", "<", "<=")
            val attribute = predicateParams[0]
                .replace("(", "")
                .replace(")", "")

            val jsonPathAttribute = if (attribute.isCompoundAttribute()) {
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

    fun createGeoQueryStatement(geoQuery: GeoQuery?, geoProperty: NgsiLdGeoProperty): String {
        val targetWKTCoordinates = geoProperty.instances[0].coordinates.value
        val georelParams = extractGeorelParams(geoQuery!!.georel)

        return if (georelParams.first == DISTANCE_QUERY_CLAUSE)
            """
            SELECT ST_Distance('${geoQuery.pgisGeometry}'::geography, 
                'SRID=4326;$targetWKTCoordinates'::geography) ${georelParams.second} ${georelParams.third} as match
            """.trimIndent()
        else
            """
            SELECT ST_${georelParams.first}('${geoQuery.pgisGeometry}', ST_GeomFromText('$targetWKTCoordinates')) 
                as match
            """.trimIndent()
    }

    fun extractGeorelParams(georel: String): Triple<String, String?, String?> {
        if (georel.contains(NEAR_QUERY_CLAUSE)) {
            val comparisonParams = georel.split(";")[1].split("==")
            return when (comparisonParams[0]) {
                MAX_DISTANCE_QUERY_CLAUSE -> Triple(DISTANCE_QUERY_CLAUSE, "<=", comparisonParams[1])
                MIN_DISTANCE_QUERY_CLAUSE -> Triple(DISTANCE_QUERY_CLAUSE, ">=", comparisonParams[1])
                // defaulting to an equality, maybe we should raise a 400 at creation time?
                else -> Triple(DISTANCE_QUERY_CLAUSE, "==", comparisonParams[1])
            }
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
