package com.egm.datahub.context.subscription.utils

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import org.slf4j.LoggerFactory

object NgsiLdParsingUtils {

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
}