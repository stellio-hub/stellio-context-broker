package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.JsonLdEntity

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
