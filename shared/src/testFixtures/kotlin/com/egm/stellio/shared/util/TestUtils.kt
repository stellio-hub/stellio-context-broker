package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.*
import org.springframework.core.io.ClassPathResource
import java.net.URI

fun loadSampleData(filename: String = "beehive.jsonld"): String {
    val sampleData = ClassPathResource("/ngsild/$filename")
    return String(sampleData.inputStream.readAllBytes())
}

suspend fun String.sampleDataToNgsiLdEntity(): Either<APIException, Pair<JsonLdEntity, NgsiLdEntity>> {
    val jsonLdEntity = JsonLdUtils.expandJsonLdEntity(this)
    return when (val ngsiLdEntity = jsonLdEntity.toNgsiLdEntity()) {
        is Either.Left -> BadRequestDataException("Invalid NGSI-LD input for sample data: $this").left()
        is Either.Right -> Pair(jsonLdEntity, ngsiLdEntity.value).right()
    }
}

fun String.removeNoise(): String =
    this.trim().replace("\n", "").replace(" ", "")

fun String.matchContent(other: String?): Boolean =
    this.removeNoise() == other?.removeNoise()

fun List<URI>.toListOfString(): List<String> =
    this.map { it.toString() }
