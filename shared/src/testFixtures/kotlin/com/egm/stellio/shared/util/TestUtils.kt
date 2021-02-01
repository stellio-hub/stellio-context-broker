package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.NgsiLdGeoProperty
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.EGM_BASE_CONTEXT_URL
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import org.springframework.core.io.ClassPathResource

val DEFAULT_CONTEXTS = listOf(
    "https://fiware.github.io/data-models/context.jsonld",
    NGSILD_CORE_CONTEXT
)

val AQUAC_COMPOUND_CONTEXT = "$EGM_BASE_CONTEXT_URL/aquac/jsonld-contexts/aquac-compound.jsonld"
val APIC_COMPOUND_CONTEXT = "$EGM_BASE_CONTEXT_URL/apic/jsonld-contexts/apic-compound.jsonld"

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
    latitude: Double
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
                            $latitude
                        ]
                    }
                }
            }
        """.trimIndent()

    return parseToNgsiLdAttributes(
        JsonLdUtils.expandJsonLdFragment(locationFragment, DEFAULT_CONTEXTS)
    )[0] as NgsiLdGeoProperty
}
