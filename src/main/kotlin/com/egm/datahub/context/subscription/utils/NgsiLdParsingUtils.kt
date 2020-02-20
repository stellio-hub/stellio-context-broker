package com.egm.datahub.context.subscription.utils

import com.egm.datahub.context.subscription.model.Subscription
import com.egm.datahub.context.subscription.web.BadRequestDataException
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.core.JsonLdOptions
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
}