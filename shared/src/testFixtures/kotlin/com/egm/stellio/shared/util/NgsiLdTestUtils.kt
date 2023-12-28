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

    return JsonLdUtils.expandJsonLdEntity(entityWithLocation, NGSILD_TEST_CORE_CONTEXTS)
}

suspend fun gimmeSimpleEntityWithAttributes(
    id: String,
    type: String,
    contexts: List<String> = APIC_COMPOUND_CONTEXTS
): ExpandedEntity =
    gimmeSimpleEntityWithAttributes(id, listOf(type), contexts)

suspend fun gimmeSimpleEntityWithAttributes(
    id: String,
    types: List<String>,
    contexts: List<String> = APIC_COMPOUND_CONTEXTS
): ExpandedEntity {
    val entity = mapOf(
        "id" to id,
        "type" to types
    )

    return JsonLdUtils.expandJsonLdEntity(entity, contexts)
}
