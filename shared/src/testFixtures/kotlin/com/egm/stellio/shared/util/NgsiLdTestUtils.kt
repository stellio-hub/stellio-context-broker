package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.QueryParams

fun gimmeSimpleEntityWithGeoProperty(
    propertyKey: String,
    longitude: Double,
    latitude: Double
): JsonLdEntity {
    val locationFragment =
        """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "type": "Entity",
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

    return JsonLdUtils.expandJsonLdEntity(locationFragment, DEFAULT_CONTEXTS)
}

fun buildDefaultQueryParams(): QueryParams =
    QueryParams(limit = 0, offset = 50, context = APIC_COMPOUND_CONTEXT)
