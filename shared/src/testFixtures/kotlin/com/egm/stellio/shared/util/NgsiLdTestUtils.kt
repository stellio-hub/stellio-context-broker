package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.ExpandedEntity

suspend fun gimmeSimpleEntityWithGeoProperty(
    propertyKey: String,
    longitude: Double,
    latitude: Double
): ExpandedEntity {
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

suspend fun gimmeSimpleEntityWithAttributes(
    id: String,
    type: String,
    contexts: List<String> = listOf(APIC_COMPOUND_CONTEXT)
): ExpandedEntity =
    gimmeSimpleEntityWithAttributes(id, listOf(type), contexts)

suspend fun gimmeSimpleEntityWithAttributes(
    id: String,
    types: List<String>,
    contexts: List<String> = listOf(APIC_COMPOUND_CONTEXT)
): ExpandedEntity {
    val entity = mapOf(
        "id" to id,
        "type" to types
    )

    return JsonLdUtils.expandJsonLdEntity(entity, contexts)
}
