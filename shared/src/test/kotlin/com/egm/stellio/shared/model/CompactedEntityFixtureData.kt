package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap

object CompactedEntityFixtureData {

    val normalizedEntity: CompactedEntity =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "brandName": {
                "type": "Property",
                "value": "Mercedes"
            },
            "isParked": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:OffStreetParking:Downtown1",
                "observedAt": "2017-07-29T12:00:04Z",
                "providedBy": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Person:Bob"
                }
            },
           "location": {
              "type": "GeoProperty",
              "value": {
                 "type": "Point",
                 "coordinates": [
                    24.30623,
                    60.07966
                 ]
              }
           },
            "@context": [
                "https://example.org/ngsi-ld/latest/commonTerms.jsonld",
                "https://example.org/ngsi-ld/latest/vehicle.jsonld",
                "https://example.org/ngsi-ld/latest/parking.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            ]
        }
        """.trimIndent().deserializeAsMap()

    val normalizedMultiAttributeEntity: CompactedEntity =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "speed": [
                {
                    "type": "Property",
                    "datasetId": "urn:ngsi-ld:Dataset:01",
                    "value": 10
                },
                {
                    "type": "Property",
                    "datasetId": "urn:ngsi-ld:Dataset:02",
                    "value": 11
                }
            ],
            "hasOwner": [
                {
                    "type": "Relationship",
                    "datasetId": "urn:ngsi-ld:Dataset:01",
                    "object": "urn:ngsi-ld:Person:John"
                },
                {
                    "type": "Relationship",
                    "datasetId": "urn:ngsi-ld:Dataset:02",
                    "object": "urn:ngsi-ld:Person:Jane"
                }
            ]
        }
        """.trimIndent().deserializeAsMap()
}
