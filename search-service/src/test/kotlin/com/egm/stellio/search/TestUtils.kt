package com.egm.stellio.search

import com.egm.stellio.shared.util.NgsiLdParsingUtils
import org.springframework.core.io.ClassPathResource

fun loadAndParseSampleData(filename: String = "beehive.jsonld"): Pair<Map<String, Any>, List<String>> {
    val sampleData = ClassPathResource("/ngsild/$filename")
    return NgsiLdParsingUtils.parseEntity(String(sampleData.inputStream.readAllBytes()))
}
