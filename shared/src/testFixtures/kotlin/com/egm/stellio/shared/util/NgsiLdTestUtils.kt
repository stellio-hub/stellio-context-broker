package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.NgsiLdGeoProperty
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.model.parseToNgsiLdAttributes

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

    return parseToNgsiLdAttributes(
        JsonLdUtils.expandJsonLdFragment(
            locationFragment,
            DEFAULT_CONTEXTS
        )
    )[0] as NgsiLdGeoProperty
}

fun buildDefaultQueryParams(): QueryParams =
    QueryParams(limit = 0, offset = 50, context = APIC_COMPOUND_CONTEXT)
