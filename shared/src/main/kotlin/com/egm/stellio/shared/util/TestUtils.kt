package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.JsonLdExpandedEntity
import org.springframework.core.io.ClassPathResource

fun loadAndParseSampleData(filename: String = "beehive.jsonld"): JsonLdExpandedEntity {
    val sampleData = ClassPathResource("/ngsild/$filename")
    return NgsiLdParsingUtils.parseEntity(String(sampleData.inputStream.readAllBytes()))
}

fun loadSampleData(filename: String = "beehive.jsonld"): String {
    val sampleData = ClassPathResource("/ngsild/$filename")
    return String(sampleData.inputStream.readAllBytes())
}
