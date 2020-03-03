package com.egm.datahub.context.search

import com.egm.datahub.context.search.util.NgsiLdParsingUtils
import org.springframework.core.io.ClassPathResource

fun loadAndParseSampleData(filename: String = "beehive.jsonld"): Pair<Map<String, Any>, List<String>> {
    val sampleData = ClassPathResource("/ngsild/$filename")
    return NgsiLdParsingUtils.parseEntity(String(sampleData.inputStream.readAllBytes()))
}
