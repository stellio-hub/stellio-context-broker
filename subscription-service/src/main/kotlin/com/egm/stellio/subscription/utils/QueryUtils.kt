package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.subscription.model.GeoQuery
import java.util.regex.Pattern

object QueryUtils {

    private const val NEAR_QUERY_CLAUSE = "near"
    private const val DISTANCE_QUERY_CLAUSE = "distance"
    private const val MAX_DISTANCE_QUERY_CLAUSE = "maxDistance"
    private const val MIN_DISTANCE_QUERY_CLAUSE = "minDistance"
    private val innerRegexPattern: Pattern = Pattern.compile(".*(=~\"\\(\\?i\\)).*")

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

    fun createQueryStatement(query: String, jsonLdEntity: JsonLdEntity, contexts: List<String>): String {
        val filterQuery = buildInnerQuery(query, jsonLdEntity, contexts)
        return """
        SELECT $filterQuery AS match
        """.trimIndent()
    }

    private fun buildInnerQuery(rawQuery: String, jsonLdEntity: JsonLdEntity, contexts: List<String>): String {
        // Quick hack to allow inline options for regex expressions
        // (see https://keith.github.io/xcode-man-pages/re_format.7.html for more details)
        // When matched, parenthesis are replaced by special characters that are later restored after the main
        // qPattern regex has been processed
        val rawQueryWithPatternEscaped =
            if (rawQuery.matches(innerRegexPattern.toRegex())) {
                rawQuery.replace(innerRegexPattern.toRegex()) { matchResult ->
                    matchResult.value
                        .replace("(", "##")
                        .replace(")", "//")
                }
            } else rawQuery

        return rawQueryWithPatternEscaped.replace(qPattern.toRegex()) { matchResult ->
            // restoring the eventual inline options for regex expressions (replaced above)
            val fixedValue = matchResult.value
                .replace("##", "(")
                .replace("//", ")")
            val query = extractComparisonParametersFromQuery(fixedValue)
            val targetValue = query.third.prepareDateValue(query.second).replaceSimpleQuote()

            val queryAttribute =
                if (query.first.isCompoundAttribute()) {
                    query.first.replace("]", "").split("[")
                } else {
                    query.first.split(".")
                }

            val expandedAttribute = expandJsonLdTerm(queryAttribute[0], contexts)
            val attributeType = getAttributeType(expandedAttribute, jsonLdEntity)

            if (queryAttribute.size > 1) {
                val expandSubAttribute = expandJsonLdTerm(queryAttribute[1], contexts)
                """
                jsonb_path_exists('${JsonUtils.serializeObject(jsonLdEntity.properties.toMutableMap())}',
                    '$."$expandedAttribute"."$expandSubAttribute" ?
                    (@."$JSONLD_VALUE_KW" ${query.second} $targetValue)')
                """.trimIndent()
            } else {
                when (attributeType) {
                    NGSILD_PROPERTY_TYPE ->
                        """
                        jsonb_path_exists('${JsonUtils.serializeObject(jsonLdEntity.properties.toMutableMap())}',
                            '$."$expandedAttribute"."$NGSILD_PROPERTY_VALUE" ?
                                (@."$JSONLD_VALUE_KW" ${query.second} $targetValue)')
                        """.trimIndent()

                    NGSILD_RELATIONSHIP_TYPE ->
                        """
                        jsonb_path_exists('${JsonUtils.serializeObject(jsonLdEntity.properties.toMutableMap())}',
                            '$."$expandedAttribute"."$NGSILD_RELATIONSHIP_HAS_OBJECT" ?
                                (@."$JSONLD_ID" ${query.second} $targetValue)')
                        """.trimIndent()

                    else -> throw ResourceNotFoundException("Attribute type are not food : ${attributeType.uri}")
                }
            }
        }
            .replace(";", " AND ")
            .replace("|", " OR ")
    }

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

    private fun getAttributeType(expandedAttribute: ExpandedTerm, jsonLdEntity: JsonLdEntity): AttributeType {
        val jsonLdAttribute = (jsonLdEntity.properties[expandedAttribute] as? List<Map<String, Any>>)?.get(0)
            ?: throw BadRequestDataException(
                "Unmatched query since it contains an unknown attribute $expandedAttribute"
            )
        return AttributeType((jsonLdAttribute[JSONLD_TYPE] as List<String>)[0])
    }

    fun String.isCompoundAttribute(): Boolean =
        this.contains("\\[.*?]".toRegex())
}
