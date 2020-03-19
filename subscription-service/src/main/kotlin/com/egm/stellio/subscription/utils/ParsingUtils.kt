package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.subscription.model.EntityInfo
import com.egm.stellio.subscription.model.Subscription
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils

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

fun parseSubscriptionUpdate(input: String): Pair<Map<String, Any>, List<String>?> {
    val mapper = jacksonObjectMapper()
    val parsedSubscription: Map<String, List<Any>> = mapper.readValue(input, mapper.typeFactory.constructMapLikeType(
        Map::class.java, String::class.java, Any::class.java
    ))

    return Pair(parsedSubscription, NgsiLdParsingUtils.getContextOrThrowError(input))
}

fun parseEntityInfo(input: Map<String, Any>, contexts: List<String>?): EntityInfo {
    val mapper = jacksonObjectMapper()
    val entityInfo = mapper.convertValue(input, EntityInfo::class.java)
    entityInfo.type = NgsiLdParsingUtils.expandJsonLdKey(entityInfo.type, contexts!!)!!
    return entityInfo
}

fun parseEntity(input: String): Pair<Map<String, Any>, List<String>> {
    val mapper = jacksonObjectMapper()
    val expandedEntity = JsonLdProcessor.expand(JsonUtils.fromInputStream(input.byteInputStream()))[0]

    // TODO find a way to avoid this extra parsing
    val parsedInput: Map<String, Any> = mapper.readValue(input, mapper.typeFactory.constructMapLikeType(
        Map::class.java, String::class.java, Any::class.java
    ))

    return Pair(expandedEntity as Map<String, Any>, parsedInput["@context"] as List<String>)
}