package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.NgsiLdGeoProperty
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import org.springframework.core.io.ClassPathResource

val DEFAULT_CONTEXTS = listOf("https://schema.lab.fiware.org/ld/context.jsonld")

fun parseSampleDataToNgsiLd(filename: String = "beehive.jsonld"): NgsiLdEntity =
    parseSampleDataToJsonLd(filename).toNgsiLdEntity()

fun parseSampleDataToJsonLd(filename: String = "beehive.jsonld"): JsonLdEntity =
    expandJsonLdEntity(loadSampleData(filename))

fun loadSampleData(filename: String = "beehive.jsonld"): String {
    val sampleData = ClassPathResource("/ngsild/$filename")
    return String(sampleData.inputStream.readAllBytes())
}

fun String.removeNoise(): String =
    this.trim().replace("\n", "").replace(" ", "")

fun String.matchContent(other: String?): Boolean =
    this.removeNoise() == other?.removeNoise()

fun parseLocationFragmentToPointGeoProperty(
    longitude: Double,
    latitute: Double
): NgsiLdGeoProperty {
    val locationFragment =
        """
            {
                "location": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Point",
                        "coordinates": [
                            $longitude,
                            $latitute
                        ]
                    }
                }
            }
        """.trimIndent()

    return parseToNgsiLdAttributes(
        JsonLdUtils.expandJsonLdFragment(locationFragment, DEFAULT_CONTEXTS)
    )[0] as NgsiLdGeoProperty
}
