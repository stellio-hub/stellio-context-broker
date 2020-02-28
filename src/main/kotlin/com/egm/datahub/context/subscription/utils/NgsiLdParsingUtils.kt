package com.egm.datahub.context.subscription.utils

import com.egm.datahub.context.subscription.model.Subscription
import com.egm.datahub.context.subscription.web.BadRequestDataException
import com.egm.datahub.context.subscription.web.InvalidQueryException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import org.slf4j.LoggerFactory

object NgsiLdParsingUtils {

    const val NGSILD_LOCATION_PROPERTY = "https://uri.etsi.org/ngsi-ld/location"
    const val NGSILD_COORDINATES_PROPERTY = "https://uri.etsi.org/ngsi-ld/coordinates"
    const val NGSILD_GEOPROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    const val NGSILD_POINT_PROPERTY = "Point"

    private val logger = LoggerFactory.getLogger(javaClass)

    fun parseEntity(input: String): Pair<Map<String, Any>, List<String>> {
        val expandedEntity = JsonLdProcessor.expand(JsonUtils.fromInputStream(input.byteInputStream()))[0]

        val expandedResult = JsonUtils.toPrettyString(expandedEntity)
        logger.debug("Expanded entity is $expandedResult")

        // TODO find a way to avoid this extra parsing
        val mapper = jacksonObjectMapper()
        val parsedInput: Map<String, Any> = mapper.readValue(input, mapper.typeFactory.constructMapLikeType(
            Map::class.java, String::class.java, Any::class.java
        ))

        return Pair(expandedEntity as Map<String, Any>, parsedInput["@context"] as List<String>)
    }

    fun expandJsonLdKey(type: String, contexts: List<String>): String? {
        val jsonLdOptions = JsonLdOptions()
        jsonLdOptions.expandContext = mapOf("@context" to contexts)
        val expandedType = JsonLdProcessor.expand(mapOf(type to mapOf<String, Any>()), jsonLdOptions)
        logger.debug("Expanded type $type to $expandedType")
        return if (expandedType.isNotEmpty())
            (expandedType[0] as Map<String, Any>).keys.first()
        else
            null
    }

    fun parseSubscription(input: String, context: List<String>): Subscription {
        val mapper = jacksonObjectMapper()
        val rawParsedData = mapper.readTree(input) as ObjectNode
        if (rawParsedData.get("@context") != null)
            rawParsedData.remove("@context")

        try {
            val subscription = mapper.readValue(rawParsedData.toString(), Subscription::class.java)
            subscription.expandTypes(context)
            return subscription
        } catch (e: Exception) {
            throw BadRequestDataException(e.message ?: "Failed to parse subscription")
        }
    }

    fun getContextOrThrowError(input: String): List<String> {
        val mapper = jacksonObjectMapper()
        val rawParsedData = mapper.readTree(input) as ObjectNode
        val context = rawParsedData.get("@context") ?: throw BadRequestDataException("Context not provided ")

        return mapper.readValue(context.toString(), mapper.typeFactory.constructCollectionType(List::class.java, String::class.java))
    }

    fun expandValueAsMap(value: Any): Map<String, List<Any>> =
            (value as List<Any>)[0] as Map<String, List<Any>>

    fun getLocationFromEntity(parsedEntity: Pair<Map<String, Any>, List<String>>): Map<String, Any> {
        val location = expandValueAsMap(parsedEntity.first[NGSILD_LOCATION_PROPERTY]!!)
        val locationValue = expandValueAsMap(location[NGSILD_GEOPROPERTY_VALUE]!!)
        val geoPropertyType = locationValue["@type"]!![0] as String
        val geoPropertyValue = locationValue[NGSILD_COORDINATES_PROPERTY]!!
        if (geoPropertyType.extractShortTypeFromExpanded() == NGSILD_POINT_PROPERTY) {
            val longitude = (geoPropertyValue[0] as Map<String, Double>)["@value"]
            val latitude = (geoPropertyValue[1] as Map<String, Double>)["@value"]
            return mapOf("geometry" to geoPropertyType.extractShortTypeFromExpanded(), "coordinates" to listOf(longitude, latitude))
        } else {
            var res = arrayListOf<List<Double?>>()
            var count = 1
            geoPropertyValue.forEach {
                if (count % 2 != 0) {
                    val longitude = (geoPropertyValue[count - 1] as Map<String, Double>)["@value"]
                    val latitude = (geoPropertyValue[count] as Map<String, Double>)["@value"]
                    res.add(listOf(longitude, latitude))
                }
                count ++
            }
            return mapOf("geometry" to geoPropertyType.extractShortTypeFromExpanded(), "coordinates" to res)
        }
    }

    fun String.extractShortTypeFromExpanded(): String =
    // TODO is it always after a '/' ? can't it be after a '#' ? (https://redmine.eglobalmark.com/issues/852)
            // TODO do a clean implementation using info from @context
            this.substringAfterLast("/").substringAfterLast("#")

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
                throw InvalidQueryException(e.message ?: "Unresolved value $it")
            }
        }

        return node.get("type")
    }
}