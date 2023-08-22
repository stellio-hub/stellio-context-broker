package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.Some
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import org.springframework.core.io.ClassPathResource
import java.net.URI

fun loadSampleData(filename: String = "beehive.jsonld"): String {
    val sampleData = ClassPathResource("/ngsild/$filename")
    return String(sampleData.inputStream.readAllBytes())
}

fun loadMinimalEntity(
    entityId: URI,
    entityTypes: Set<String>,
    contexts: Set<String> = setOf(NGSILD_CORE_CONTEXT)
): String =
    """
        {
            "id": "$entityId",
            "type": [${entityTypes.joinToString(",") { "\"$it\"" }}],
            "@context": [${contexts.joinToString(",") { "\"$it\"" }}]
        }
    """.trimIndent()

fun loadMinimalEntityWithSap(
    entityId: URI,
    entityTypes: Set<String>,
    specificAccessPolicy: AuthContextModel.SpecificAccessPolicy,
    contexts: Set<String> = setOf(NGSILD_CORE_CONTEXT)
): String =
    """
        {
            "id": "$entityId",
            "type": [${entityTypes.joinToString(",") { "\"$it\"" }}],
            "specificAccessPolicy": {
                "type": "Property",
                "value": "${specificAccessPolicy.name}"
            },
            "@context": [${contexts.joinToString(",") { "\"$it\"" }}]
        }
    """.trimIndent()

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

const val MOCK_USER_SUB = "60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"
val sub = Some(MOCK_USER_SUB)
