package com.egm.stellio.shared.util

import org.springframework.core.io.ClassPathResource

fun loadAndParseSampleData(filename: String = "beehive.jsonld"): Pair<Map<String, Any>, List<String>> {
    val sampleData = ClassPathResource("/ngsild/$filename")
    return NgsiLdParsingUtils.parseEntity(String(sampleData.inputStream.readAllBytes()))
}

fun loadSampleData(filename: String = "beehive.jsonld"): String {
    val sampleData = ClassPathResource("/ngsild/$filename")
    return String(sampleData.inputStream.readAllBytes())
}
