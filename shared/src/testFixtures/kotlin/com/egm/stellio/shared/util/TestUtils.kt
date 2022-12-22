package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import org.springframework.core.io.ClassPathResource

const val EMPTY_PAYLOAD = "{}"

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

fun parseGeoFragmentToPointGeoProperty(
    propertyKey: String,
    longitude: Double,
    latitude: Double
): NgsiLdGeoProperty {
    val locationFragment =
        """
            {
                "$propertyKey": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Point",
                        "coordinates": [
                            $longitude,
                            $latitude
                        ]
                    }
                }
            }
        """.trimIndent()

    return parseToNgsiLdAttributes(expandJsonLdFragment(locationFragment, DEFAULT_CONTEXTS))[0] as NgsiLdGeoProperty
}
