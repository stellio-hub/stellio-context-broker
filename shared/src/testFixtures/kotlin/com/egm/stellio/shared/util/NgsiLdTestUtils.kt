package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.QueryParams

suspend fun gimmeSimpleEntityWithGeoProperty(
    propertyKey: String,
    longitude: Double,
    latitude: Double
): JsonLdEntity {
    val entityWithLocation =
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

    return JsonLdUtils.expandJsonLdEntity(entityWithLocation, DEFAULT_CONTEXTS)
}

fun buildDefaultQueryParams(): QueryParams =
    QueryParams(limit = 0, offset = 50, context = APIC_COMPOUND_CONTEXT)
