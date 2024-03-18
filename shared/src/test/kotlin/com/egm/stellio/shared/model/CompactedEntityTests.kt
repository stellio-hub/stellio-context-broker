package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType

class CompactedEntityTests {

    private val normalizedJson =
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
        """.trimIndent()

    private val simplifiedJson =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "brandName": "Mercedes",
            "isParked": "urn:ngsi-ld:OffStreetParking:Downtown1",
            "location": {
             "type": "Point",
             "coordinates": [
                24.30623,
                60.07966
             ]
           },
            "@context": [
                "https://example.org/ngsi-ld/latest/commonTerms.jsonld",
                "https://example.org/ngsi-ld/latest/vehicle.jsonld",
                "https://example.org/ngsi-ld/latest/parking.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            ]
        }
        """.trimIndent()

    private val normalizedMultiAttributeJson =
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
        """.trimIndent()

    private val simplifiedMultiAttributeJson =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "speed": {
                  "dataset": {
                       "urn:ngsi-ld:Dataset:01": 10,
                       "urn:ngsi-ld:Dataset:02": 11
                   }
            },
            "hasOwner": {
                  "dataset": {
                       "urn:ngsi-ld:Dataset:01": "urn:ngsi-ld:Person:John",
                       "urn:ngsi-ld:Dataset:02": "urn:ngsi-ld:Person:Jane"
                   }
            }
        }
        """.trimIndent()

    @Test
    fun `it should simplify a compacted entity`() {
        val normalizedMap = normalizedJson.deserializeAsMap()
        val simplifiedMap = simplifiedJson.deserializeAsMap()

        val resultMap = normalizedMap.toKeyValues()

        Assertions.assertEquals(simplifiedMap, resultMap)
    }

    @Test
    fun `it should simplify a compacted entity with multi-attributes`() {
        val normalizedMap = normalizedMultiAttributeJson.deserializeAsMap()
        val simplifiedMap = simplifiedMultiAttributeJson.deserializeAsMap()

        val resultMap = normalizedMap.toKeyValues()

        Assertions.assertEquals(simplifiedMap, resultMap)
    }

    @Test
    fun `it should return the simplified representation of a JsonProperty having an object as value`() {
        val compactedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "jsonProperty": {
                    "type": "JsonProperty",
                    "json": {
                        "anId": "id",
                        "aNullValue": null,
                        "anArray": [1, 2]
                    }
                }
            }
        """.trimIndent()
            .deserializeAsMap()

        val simplifiedRepresentation = compactedEntity.toKeyValues()

        val expectedSimplifiedRepresentation = """
            {
               "id": "urn:ngsi-ld:Entity:01",
               "type": "Entity",
               "jsonProperty": {
                  "json": {
                     "anId": "id",
                     "aNullValue": null,
                     "anArray": [1, 2]
                  }
               }
            }
        """.trimIndent()
        assertJsonPayloadsAreEqual(expectedSimplifiedRepresentation, serializeObject(simplifiedRepresentation))
    }

    @Test
    fun `it should return the simplified representation of a JsonProperty having an array as value`() {
        val compactedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "jsonProperty": {
                    "type": "JsonProperty",
                    "json": [
                        { "anId": "id" },
                        { "anotherId": "anotherId" }
                    ]
                }
            }
        """.trimIndent()
            .deserializeAsMap()

        val simplifiedRepresentation = compactedEntity.toKeyValues()

        val expectedSimplifiedRepresentation = """
            {
               "id": "urn:ngsi-ld:Entity:01",
               "type": "Entity",
               "jsonProperty": {
                  "json": [
                      { "anId": "id" },
                      { "anotherId": "anotherId" }
                  ]
               }
            }
        """.trimIndent()
        assertJsonPayloadsAreEqual(expectedSimplifiedRepresentation, serializeObject(simplifiedRepresentation))
    }

    @Test
    fun `it should return a simplified entity with sysAttrs`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "createdAt": "2023-11-25T08:00:00Z",
                "modifiedAt": "2023-11-25T09:00:00Z",
                "brandName": {
                    "type": "Property",
                    "value": "Mercedes"
                }
            }
            """.trimIndent().deserializeAsMap()
        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "createdAt": "2023-11-25T08:00:00Z",
                "modifiedAt": "2023-11-25T09:00:00Z",
                "brandName": "Mercedes"
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.JSON,
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = true
            )
        )

        Assertions.assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity without sysAttrs`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "createdAt": "2023-11-25T08:00:00Z",
                "modifiedAt": "2023-11-25T09:00:00Z",
                "brandName": {
                    "type": "Property",
                    "value": "Mercedes"
                }
            }
            """.trimIndent().deserializeAsMap()
        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "brandName": "Mercedes"
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.forMediaType(MediaType.APPLICATION_JSON),
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false
            )
        )

        Assertions.assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity with a Multi-Attribute of type Property`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "speed": [
                {
                     "type": "Property",
                     "value": 55,
                     "datasetId": "urn:ngsi-ld:Property:speedometerA4567-speed"
                },
                {
                     "type": "Property",
                     "value": 54.5,
                     "datasetId": "urn:ngsi-ld:Property:gpsBxyz123-speed"
                }]
            }
            """.trimIndent().deserializeAsMap()
        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "speed": {
                   "dataset": {
                       "urn:ngsi-ld:Property:speedometerA4567-speed": 55,
                       "urn:ngsi-ld:Property:gpsBxyz123-speed": 54.5
                   }
                }
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.forMediaType(MediaType.APPLICATION_JSON),
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false
            )
        )

        Assertions.assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity with a Multi-Attribute of type Relationship`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Event:bonjourLeMonde",
                "sameAs": [
                {
                     "type": "Relationship",
                     "object": "urn:ngsi-ld:Event:helloWorld",
                     "datasetId": "urn:ngsi-ld:Relationship:1"
                },
                {
                     "type": "Relationship",
                     "object": "urn:ngsi-ld:Event:halloWelt",
                     "datasetId": "urn:ngsi-ld:Relationship:2"
                }]
            }
            """.trimIndent().deserializeAsMap()
        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Event:bonjourLeMonde",
                "sameAs": {
                   "dataset": {
                       "urn:ngsi-ld:Relationship:1": "urn:ngsi-ld:Event:helloWorld",
                       "urn:ngsi-ld:Relationship:2": "urn:ngsi-ld:Event:halloWelt"
                   }
                }
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.forMediaType(MediaType.APPLICATION_JSON),
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false
            )
        )

        Assertions.assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity with a Multi-Attribute of type JsonProperty`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "speed": [
                {
                     "type": "JsonProperty",
                     "json": {
                        "anId": "id",
                        "anArray": [1, 2]
                    },
                     "datasetId": "urn:ngsi-ld:JsonProperty:1"
                },
                {
                     "type": "JsonProperty",
                      "json": {
                        "anId": "id",
                        "anArray": [1, 2]
                    },
                     "datasetId": "urn:ngsi-ld:Property:JsonProperty:2"
                }]
            }
            """.trimIndent().deserializeAsMap()
        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "speed":
                {
                 "dataset": {
                       "urn:ngsi-ld:JsonProperty:1": {
                        "anId": "id",
                        "anArray": [1, 2]
                    },
                       "urn:ngsi-ld:Property:JsonProperty:2": {
                        "anId": "id",
                        "anArray": [1, 2]
                    }
                   }
                }
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.forMediaType(MediaType.APPLICATION_JSON),
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false
            )
        )

        Assertions.assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity with a Multi-Attribute of type GeoProperty`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "location": [
                {
                     "type": "GeoProperty",
                     "value": {
                         "type": "Point",
                         "coordinates": [
                            24.30623,
                            60.07966
                         ]
                      },
                     "datasetId": "urn:ngsi-ld:GeoProperty:1"
                },
                {
                     "type": "GeoProperty",
                     "value": {
                         "type": "Point",
                         "coordinates": [
                            25.30623,
                            60.08066
                         ]
                      },
                     "datasetId": "urn:ngsi-ld:GeoProperty:2"
                }]
            }
            """.trimIndent().deserializeAsMap()
        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "location":
                {
                 "dataset": {
                       "urn:ngsi-ld:GeoProperty:1": {
                         "type": "Point",
                         "coordinates": [
                            24.30623,
                            60.07966
                         ]
                      },
                       "urn:ngsi-ld:GeoProperty:2": {
                         "type": "Point",
                         "coordinates": [
                            25.30623,
                            60.08066
                         ]
                      }
                   }
                }
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.forMediaType(MediaType.APPLICATION_JSON),
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false
            )
        )

        Assertions.assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity with a Multi-Attribute with no datasetId`() {
        val inputEntity =
            """
            {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "speed": [
                {
                    "type": "Property",
                    "value": 10
                },
                {
                    "type": "Property",
                    "datasetId": "urn:ngsi-ld:Dataset:01",
                    "value": 11
                }
            ],
            "hasOwner": [
                {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Person:John"
                },
                {
                    "type": "Relationship",
                    "datasetId": "urn:ngsi-ld:Dataset:01",
                    "object": "urn:ngsi-ld:Person:Jane"
                }
            ]
        }
            """.trimIndent().deserializeAsMap()
        val expectedEntity =
            """
           {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "speed": {
                  "dataset": {
                       "@none": 10,
                       "urn:ngsi-ld:Dataset:01": 11
                   }
            },
            "hasOwner": {
                  "dataset": {
                       "@none": "urn:ngsi-ld:Person:John",
                       "urn:ngsi-ld:Dataset:01": "urn:ngsi-ld:Person:Jane"
                   }
            }
        }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.forMediaType(MediaType.APPLICATION_JSON),
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false
            )
        )

        Assertions.assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a normalized GeoJSON entity on location attribute`() {
        val inputEntity = normalizedJson.deserializeAsMap()

        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Feature",
                "geometry": {
                     "type": "Point",
                     "coordinates": [ 24.30623, 60.07966 ]
                },
                "properties": {
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
            }
            """.trimIndent().deserializeAsMap()

        val actualEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.GEO_JSON,
                AttributeRepresentation.NORMALIZED,
                includeSysAttrs = false,
                geometryProperty = JsonLdUtils.NGSILD_LOCATION_TERM
            )
        )

        Assertions.assertEquals(expectedEntity, actualEntity)
    }

    @Test
    fun `it should return a simplified GeoJSON entity on location attribute`() {
        val inputEntity = normalizedJson.deserializeAsMap()

        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Feature",
                "geometry": {
                     "type": "Point",
                     "coordinates": [ 24.30623, 60.07966 ]
                },
                "properties": {
                    "type": "Vehicle",
                    "brandName": "Mercedes",
                    "isParked": "urn:ngsi-ld:OffStreetParking:Downtown1",
                    "location": {
                        "type": "Point",
                        "coordinates": [ 24.30623, 60.07966 ]
                    },
                    "@context": [
                        "https://example.org/ngsi-ld/latest/commonTerms.jsonld",
                        "https://example.org/ngsi-ld/latest/vehicle.jsonld",
                        "https://example.org/ngsi-ld/latest/parking.jsonld",
                        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
                    ]
                }
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.GEO_JSON,
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false,
                geometryProperty = JsonLdUtils.NGSILD_LOCATION_TERM
            )
        )

        Assertions.assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a GeoJSON entity with a null geometry if the GeoProperty does not exist`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "brandName": {
                    "type": "Property",
                    "value": "Mercedes"
                }
            }
            """.trimIndent().deserializeAsMap()

        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Feature",
                "geometry": null,
                "properties": {
                    "type": "Vehicle",
                    "brandName": "Mercedes"
                }
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.GEO_JSON,
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false,
                geometryProperty = JsonLdUtils.NGSILD_LOCATION_TERM
            )
        )

        Assertions.assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return simplified GeoJSON entities`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "location": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Point",
                        "coordinates": [ 24.30623, 60.07966 ]
                    }
                }
            }
            """.trimIndent().deserializeAsMap()

        val expectedEntities =
            """
            {
                "type": "FeatureCollection",
                "features": [{
                    "id": "urn:ngsi-ld:Vehicle:A4567",
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [ 24.30623, 60.07966 ]
                    },
                    "properties": {
                        "type": "Vehicle",
                        "location": {
                            "type": "Point",
                            "coordinates": [ 24.30623, 60.07966 ]
                        }
                    }
                }, {
                    "id": "urn:ngsi-ld:Vehicle:A4567",
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [ 24.30623, 60.07966 ]
                    },
                    "properties": {
                        "type": "Vehicle",
                        "location": {
                            "type": "Point",
                            "coordinates": [ 24.30623, 60.07966 ]
                        }                        
                    }
                }]
            }
            """.trimIndent().deserializeAsMap()

        val actualEntities = listOf(inputEntity, inputEntity).toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.GEO_JSON,
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false,
                JsonLdUtils.NGSILD_LOCATION_TERM
            )
        )

        Assertions.assertEquals(expectedEntities, actualEntities)
    }
}
