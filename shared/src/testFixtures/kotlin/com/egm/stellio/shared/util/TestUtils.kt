package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.Some
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import org.springframework.core.io.ClassPathResource
import java.net.URI
import java.time.ZonedDateTime

fun loadSampleData(filename: String = "beehive.jsonld"): String {
    val sampleData = ClassPathResource("/ngsild/$filename")
    return String(sampleData.inputStream.readAllBytes())
}

suspend fun loadAndExpandSampleData(filename: String = "beehive.jsonld"): ExpandedEntity {
    val sampleData = ClassPathResource("/ngsild/$filename")
    return expandJsonLdEntity(String(sampleData.inputStream.readAllBytes()))
}

suspend fun loadAndPrepareSampleData(
    filename: String = "beehive.jsonld"
): Either<APIException, Pair<ExpandedEntity, NgsiLdEntity>> =
    loadSampleData(filename).sampleDataToNgsiLdEntity()

fun loadMinimalEntity(
    entityId: URI,
    entityTypes: Set<String>,
    contexts: Set<String> = setOf(NGSILD_TEST_CORE_CONTEXT)
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
    contexts: List<String> = NGSILD_TEST_CORE_CONTEXTS
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

suspend fun loadAndExpandMinimalEntity(
    id: String,
    type: String,
    attributes: String? = null,
    contexts: List<String> = APIC_COMPOUND_CONTEXTS
): ExpandedEntity =
    loadAndExpandMinimalEntity(id, listOf(type), attributes, contexts)

suspend fun loadAndExpandMinimalEntity(
    id: String,
    types: List<String>,
    attributes: String? = null,
    contexts: List<String> = APIC_COMPOUND_CONTEXTS
): ExpandedEntity =
    expandJsonLdEntity(
        """
        {
            "id": "$id",
            "type": [${types.joinToString(",") { "\"$it\"" }}]
            ${attributes?.let { ", $it" } ?: ""}
        }
        """.trimIndent(),
        contexts
    )

suspend fun loadAndExpandDeletedEntity(
    entityId: URI,
    deletedAt: ZonedDateTime? = ngsiLdDateTime(),
    contexts: List<String> = APIC_COMPOUND_CONTEXTS
): ExpandedEntity =
    expandJsonLdEntity(
        """
        {
            "id": "$entityId",
            "deletedAt": "$deletedAt"
        }
        """.trimIndent(),
        contexts
    )

suspend fun String.sampleDataToNgsiLdEntity(): Either<APIException, Pair<ExpandedEntity, NgsiLdEntity>> {
    val expandedEntity = expandJsonLdEntity(this)
    return when (val ngsiLdEntity = expandedEntity.toNgsiLdEntity()) {
        is Either.Left -> BadRequestDataException("Invalid NGSI-LD input for sample data: $this").left()
        is Either.Right -> Pair(expandedEntity, ngsiLdEntity.value).right()
    }
}

suspend fun expandJsonLdEntity(input: String): ExpandedEntity {
    val jsonInput = input.deserializeAsMap()
    return expandJsonLdEntity(jsonInput.minus(JSONLD_CONTEXT), jsonInput.extractContexts())
}

fun String.removeNoise(): String =
    this.trim().replace("\n", "").replace(" ", "")

fun String.matchContent(other: String?): Boolean =
    this.removeNoise() == other?.removeNoise()

fun List<URI>.toListOfString(): List<String> =
    this.map { it.toString() }

const val MOCK_USER_SUB = "60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"
val sub = Some(MOCK_USER_SUB)
