package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.toNgsiLdEntity
import org.springframework.core.io.ClassPathResource
import java.net.URI

fun loadSampleData(filename: String = "beehive.jsonld"): String {
    val sampleData = ClassPathResource("/ngsild/$filename")
    return String(sampleData.inputStream.readAllBytes())
}

fun String.sampleDataToNgsiLdEntity(): Pair<JsonLdEntity, NgsiLdEntity> {
    val jsonLdEntity = JsonLdUtils.expandJsonLdEntity(this)
    return Pair(jsonLdEntity, jsonLdEntity.toNgsiLdEntity())
}

fun String.removeNoise(): String =
    this.trim().replace("\n", "").replace(" ", "")

fun String.matchContent(other: String?): Boolean =
    this.removeNoise() == other?.removeNoise()

fun List<URI>.toListOfString(): List<String> =
    this.map { it.toString() }
