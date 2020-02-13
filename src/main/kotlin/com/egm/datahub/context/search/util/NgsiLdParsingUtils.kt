package com.egm.datahub.context.search.util

import com.egm.datahub.context.search.exception.InvalidNgsiLdPayloadException
import com.egm.datahub.context.search.model.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime

object NgsiLdParsingUtils {

    private val logger = LoggerFactory.getLogger(NgsiLdParsingUtils::class.java)

    const val NGSILD_PROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"

    private var mapper = jacksonObjectMapper()

    init {
        // TODO check if this registration is still required
        mapper.findAndRegisterModules()
    }

    fun parseTemporalPropertyUpdate(content: String): Observation {
        val rawParsedData = mapper.readTree(content)
        val propertyName = rawParsedData.fieldNames().next()
        val propertyValues = rawParsedData[propertyName]

        logger.debug("Received property $rawParsedData (values: $propertyValues)")
        val isContentValid = listOf("type", "value", "unitCode", "observedAt", "observedBy")
            .map { propertyValues.has(it) }
            .all { it }
        if (!isContentValid)
            throw InvalidNgsiLdPayloadException("Received content misses one or more required attributes")

        val location =
            if (propertyValues["location"] != null) {
                val coordinates = propertyValues["location"]["value"]["coordinates"].asIterable()
                Pair(coordinates.first().asDouble(), coordinates.last().asDouble())
            } else {
                null
            }

        return Observation(
            attributeName = propertyName,
            observedBy = propertyValues["observedBy"]["object"].asText(),
            observedAt = OffsetDateTime.parse(propertyValues["observedAt"].asText()),
            value = propertyValues["value"].asDouble(),
            unitCode = propertyValues["unitCode"].asText(),
            latitude = location?.first,
            longitude = location?.second
        )
    }

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

    fun expandValueAsMap(value: Any): Map<String, List<Any>> =
        (value as List<Any>)[0] as Map<String, List<Any>>

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
}
