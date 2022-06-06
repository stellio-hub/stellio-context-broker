package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import org.springframework.core.io.ClassPathResource

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

fun parseGeoFragmentToPolygonGeoProperty(
    propertyKey: String,
    coordinates: List<List<List<Double>>>
): NgsiLdGeoProperty {
    val locationFragment =
        """
            {
                "$propertyKey": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Polygon",
                        "coordinates": $coordinates
                    }
                }
            }
        """.trimIndent()

    return parseToNgsiLdAttributes(expandJsonLdFragment(locationFragment, DEFAULT_CONTEXTS))[0] as NgsiLdGeoProperty
}
