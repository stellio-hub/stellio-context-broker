package com.egm.stellio.entity.util

import com.egm.stellio.shared.util.NgsiLdParsingUtils.parseEntities
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.net.URLDecoder

fun extractComparaisonParametersFromQuery(query: String): ArrayList<String> {
    val splitted = ArrayList<String>()
    if (query.contains("==")) {
        splitted.add(query.split("==")[0])
        splitted.add("=")
        splitted.add(query.split("==")[1])
    } else if (query.contains("!=")) {
        splitted.add(query.split("!=")[0])
        splitted.add("<>")
        splitted.add(query.split("!=")[1])
    } else if (query.contains(">=")) {
        splitted.add(query.split(">=")[0])
        splitted.add(">=")
        splitted.add(query.split(">=")[1])
    } else if (query.contains(">")) {
        splitted.add(query.split(">")[0])
        splitted.add(">")
        splitted.add(query.split(">")[1])
    } else if (query.contains("<=")) {
        splitted.add(query.split("<=")[0])
        splitted.add("<=")
        splitted.add(query.split("<=")[1])
    } else if (query.contains("<")) {
        splitted.add(query.split("<")[0])
        splitted.add("<")
        splitted.add(query.split("<")[1])
    }
    return splitted
}

fun extractEntitiesFromJsonPayload(payload: String): List<Map<String, Any>> {
    val mapper = jacksonObjectMapper()
    return mapper.readValue(payload, mapper.typeFactory.constructCollectionType(MutableList::class.java, Map::class.java))
}

fun extractAndParseBatchOfEntities(payload: String): List<Pair<Map<String, Any>, List<String>>> {
    val extractedEntities = extractEntitiesFromJsonPayload(payload)
    return parseEntities(extractedEntities)
}

fun String.decode(): String =
    URLDecoder.decode(this, "UTF-8")

fun List<String>.decode(): List<String> =
    this.map {
        it.decode()
    }
