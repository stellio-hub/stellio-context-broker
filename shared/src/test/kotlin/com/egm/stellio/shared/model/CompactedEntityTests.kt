package com.egm.stellio.shared.model

import com.egm.stellio.shared.model.AttributeProjection.Companion.parsePickOmitParameters
import com.egm.stellio.shared.model.CompactedEntityFixtureData.normalizedEntity
import com.egm.stellio.shared.model.CompactedEntityFixtureData.normalizedMultiAttributeEntity
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceedAndResult
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType

class CompactedEntityTests {

    private val simplifiedEntity =
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

    private val simplifiedMultiAttributeEntity =
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
    fun `it should filter the entity members based on pick parameter`() = runTest {
        val entity = loadSampleData("beehive_with_single_attribute_instances.jsonld").deserializeAsMap()

        val expectedEntity = """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "name": {
                "type": "Property",
                "value": "beehive"
            },
            "@context": ["$APIC_COMPOUND_CONTEXT"]
        }
        """.trimIndent()

        val pickAndOmitParams = parsePickOmitParameters("id,name", null)
            .shouldSucceedAndResult()

        val filteredEntity = entity.filterPickAndOmit(
            pickAndOmitParams.first.getRootAttributesToPick(),
            pickAndOmitParams.second.getRootAttributesToOmit()
        ).shouldSucceedAndResult()

        assertJsonPayloadsAreEqual(expectedEntity, serializeObject(filteredEntity))
    }

    @Test
    fun `it should filter the entity members based on omit parameter`() = runTest {
        val entity = loadSampleData("beehive_with_single_attribute_instances.jsonld").deserializeAsMap()

        val expectedEntity = """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "type": "Entity",
            "@context": ["$APIC_COMPOUND_CONTEXT"]
        }
        """.trimIndent()

        val pickAndOmitParams = parsePickOmitParameters(null, "name,managedBy")
            .shouldSucceedAndResult()

        val filteredEntity = entity.filterPickAndOmit(
            pickAndOmitParams.first.getRootAttributesToPick(),
            pickAndOmitParams.second.getRootAttributesToOmit()
        ).shouldSucceedAndResult()

        assertJsonPayloadsAreEqual(expectedEntity, serializeObject(filteredEntity))
    }

    @Test
    fun `it should return an ResourceNotFound error when no entity member matches the pick parameter`() = runTest {
        val entity = loadSampleData("beehive_with_single_attribute_instances.jsonld").deserializeAsMap()

        val pickAndOmitParams = parsePickOmitParameters("unknown", null)
            .shouldSucceedAndResult()

        entity.filterPickAndOmit(
            pickAndOmitParams.first.getRootAttributesToPick(),
            pickAndOmitParams.second.getRootAttributesToOmit()
        ).shouldFailWith {
            it is ResourceNotFoundException &&
                it.message == "No entity member left after applying pick and omit"
        }
    }

    @Test
    fun `it should simplify a compacted entity`() {
        val simplifiedMap = simplifiedEntity.deserializeAsMap()

        val resultMap = normalizedEntity.toSimplifiedAttributes()

        assertEquals(simplifiedMap, resultMap)
    }

    @Test
    fun `it should simplify a compacted entity with multi-attributes`() {
        val simplifiedMap = simplifiedMultiAttributeEntity.deserializeAsMap()

        val resultMap = normalizedMultiAttributeEntity.toSimplifiedAttributes()

        assertEquals(simplifiedMap, resultMap)
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
        """.trimIndent().deserializeAsMap()

        val simplifiedRepresentation = compactedEntity.toSimplifiedAttributes()

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
        """.trimIndent().deserializeAsMap()

        val simplifiedRepresentation = compactedEntity.toSimplifiedAttributes()

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

        assertEquals(expectedEntity, simplifiedEntity)
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

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity with a multi-attribute Property`() {
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

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity with a multi-attribute Relationship`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
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
        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "hasOwner": {
                   "dataset": {
                       "urn:ngsi-ld:Dataset:01": "urn:ngsi-ld:Person:John",
                       "urn:ngsi-ld:Dataset:02": "urn:ngsi-ld:Person:Jane"
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

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity with a multivalued Relationship`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "hasOwner": {
                    "type": "Relationship",
                    "object": [
                        "urn:ngsi-ld:Person:John",
                        "urn:ngsi-ld:Person:Jane"
                    ]
                }
            }
            """.trimIndent().deserializeAsMap()
        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "hasOwner": [
                    "urn:ngsi-ld:Person:John",
                    "urn:ngsi-ld:Person:Jane"
                ]
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.forMediaType(MediaType.APPLICATION_JSON),
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false
            )
        )

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity with a multi-attribute JsonProperty`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "speed": [
                {
                   "type": "JsonProperty",
                   "json": {
                      "anId": "id"
                   },
                   "datasetId": "urn:ngsi-ld:JsonProperty:1"
                },
                {
                   "type": "JsonProperty",
                   "json": {
                      "anArray": [1, 2]
                   },
                   "datasetId": "urn:ngsi-ld:JsonProperty:2"
                }]
            }
            """.trimIndent().deserializeAsMap()
        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "speed": {
                    "dataset": {
                       "urn:ngsi-ld:JsonProperty:1": {
                          "json": {
                             "anId": "id"
                          }
                       },
                       "urn:ngsi-ld:JsonProperty:2": {
                          "json": {
                             "anArray": [1, 2]
                          }
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

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity with a multi-attribute GeoProperty`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "location": [
                {
                     "type": "GeoProperty",
                     "value": {
                         "type": "Point",
                         "coordinates": [ 24.30623, 60.07966 ]
                      },
                     "datasetId": "urn:ngsi-ld:GeoProperty:1"
                },
                {
                     "type": "GeoProperty",
                     "value": {
                         "type": "Point",
                         "coordinates": [ 25.30623, 60.08066 ]
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
                         "coordinates": [ 24.30623, 60.07966 ]
                      },
                      "urn:ngsi-ld:GeoProperty:2": {
                         "type": "Point",
                         "coordinates": [ 25.30623, 60.08066 ]
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

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity with a multi-Attribute having a default instance`() {
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

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return the GeoJSON representation of a normalized entity on location attribute`() {
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

        val actualEntity = normalizedEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.GEO_JSON,
                AttributeRepresentation.NORMALIZED,
                includeSysAttrs = false,
                geometryProperty = NGSILD_LOCATION_TERM
            )
        )

        assertEquals(expectedEntity, actualEntity)
    }

    @Test
    fun `it should return the GeoJSON representation of a simplified entity on location attribute`() {
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

        val simplifiedEntity = normalizedEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.GEO_JSON,
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false,
                geometryProperty = NGSILD_LOCATION_TERM
            )
        )

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return the GeoJSON representation of an entity with a null geometry if it does not exist`() {
        val inputEntity = """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "brandName": {
                    "type": "Property",
                    "value": "Mercedes"
                },
                "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            }
        """.trimIndent().deserializeAsMap()

        val expectedEntity = """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Feature",
                "geometry": null,
                "properties": {
                    "type": "Vehicle",
                    "brandName": "Mercedes"
                },
                "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            }
        """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.GEO_JSON,
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false,
                geometryProperty = NGSILD_LOCATION_TERM
            )
        )

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return the GeoJSON representation of simplified entities`() {
        val inputEntity = """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "location": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Point",
                        "coordinates": [ 24.30623, 60.07966 ]
                    }
                },
                "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            }
        """.trimIndent().deserializeAsMap()

        val expectedEntityAsFeature = """
            {
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
                },
                "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            }
        """.trimIndent()
        val expectedEntities = """
            {
                "type": "FeatureCollection",
                "features": [
                    $expectedEntityAsFeature,
                    $expectedEntityAsFeature
                ]
            }
        """.trimIndent().deserializeAsMap()

        val actualEntities = listOf(inputEntity, inputEntity).toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.GEO_JSON,
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false,
                geometryProperty = NGSILD_LOCATION_TERM
            )
        )

        assertEquals(expectedEntities, actualEntities)
    }

    @Test
    fun `it should return the simplified representation of a LanguageProperty`() {
        val compactedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "languageProperty": {
                    "type": "LanguageProperty",
                    "languageMap": {
                        "fr": "Grand Place",
                        "nl": "Grote Markt"
                    }
                }
            }
        """.trimIndent().deserializeAsMap()

        val simplifiedRepresentation = compactedEntity.toSimplifiedAttributes()

        val expectedSimplifiedRepresentation = """
            {
               "id": "urn:ngsi-ld:Entity:01",
               "type": "Entity",
               "languageProperty": {
                    "languageMap": {
                        "fr": "Grand Place",
                        "nl": "Grote Markt"
                    }
               }
            }
        """.trimIndent()

        assertJsonPayloadsAreEqual(expectedSimplifiedRepresentation, serializeObject(simplifiedRepresentation))
    }

    @Test
    fun `it should return the simplified representation of a VocabProperty - string value`() {
        val compactedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "vocabProperty": {
                    "type": "VocabProperty",
                    "vocab": "stellio"
                }
            }
        """.trimIndent().deserializeAsMap()

        val simplifiedRepresentation = compactedEntity.toSimplifiedAttributes()

        val expectedSimplifiedRepresentation = """
            {
               "id": "urn:ngsi-ld:Entity:01",
               "type": "Entity",
               "vocabProperty": {
                    "vocab": "stellio"
               }
            }
        """.trimIndent()

        assertJsonPayloadsAreEqual(expectedSimplifiedRepresentation, serializeObject(simplifiedRepresentation))
    }

    @Test
    fun `it should return the simplified representation of a VocabProperty - array of strings value`() {
        val compactedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "vocabProperty": {
                    "type": "VocabProperty",
                    "vocab": ["stellio", "egm"]
                }
            }
        """.trimIndent().deserializeAsMap()

        val simplifiedRepresentation = compactedEntity.toSimplifiedAttributes()

        val expectedSimplifiedRepresentation = """
            {
               "id": "urn:ngsi-ld:Entity:01",
               "type": "Entity",
               "vocabProperty": {
                    "vocab": ["stellio", "egm"]
               }
            }
        """.trimIndent()

        assertJsonPayloadsAreEqual(expectedSimplifiedRepresentation, serializeObject(simplifiedRepresentation))
    }

    @Test
    fun `it should find the value of a property`() = runTest {
        val compactedAttributeInstance = mapOf(
            "type" to "Property",
            "value" to 12
        )

        compactedAttributeInstance.getTypeAndValue().let {
            assertEquals(NGSILD_PROPERTY_TERM, it.first)
            assertEquals(12, it.second)
        }
    }

    @Test
    fun `it should find the value of a language property`() = runTest {
        val compactedAttributeInstance = mapOf(
            "type" to "LanguageProperty",
            "languageMap" to mapOf("fr" to "Tour Eiffel", "en" to "Eiffel Tower")
        )

        compactedAttributeInstance.getTypeAndValue().let {
            assertEquals(NGSILD_LANGUAGEPROPERTY_TERM, it.first)
            assertEquals(2, (it.second as Map<*, *>).size)
        }
    }

    @Test
    fun `toConciseAttributes should collapse a simple Property to a bare scalar`() {
        val entity = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "speed": {
                    "type": "Property",
                    "value": 120
                }
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "speed": 120
            }
        """.trimIndent()

        val result = entity.toConciseAttributes()

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }

    @Test
    fun `toConciseAttributes should keep map with observedAt for a Property`() {
        val entity = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "speed": {
                    "type": "Property",
                    "value": 120,
                    "observedAt": "2024-01-01T00:00:00Z"
                }
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "speed": {
                    "value": 120,
                    "observedAt": "2024-01-01T00:00:00Z"
                }
            }
        """.trimIndent()

        val result = entity.toConciseAttributes()

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }

    @Test
    fun `toConciseAttributes should remove type from Property and recurse into sub-attributes`() {
        val entity = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "temperature": {
                    "type": "Property",
                    "value": 22.5,
                    "observedAt": "2024-01-01T00:00:00Z",
                    "measuredBy": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:Sensor:01"
                    }
                }
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "temperature": {
                    "value": 22.5,
                    "observedAt": "2024-01-01T00:00:00Z",
                    "measuredBy": {
                        "object": "urn:ngsi-ld:Sensor:01"
                    }
                }
            }
        """.trimIndent()

        val result = entity.toConciseAttributes()

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }

    @Test
    fun `toConciseAttributes should collapse a simple GeoProperty to a bare GeoJSON object`() {
        val entity = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "location": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Point",
                        "coordinates": [24.30623, 60.07966]
                    }
                }
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "location": {
                    "type": "Point",
                    "coordinates": [24.30623, 60.07966]
                }
            }
        """.trimIndent()

        val result = entity.toConciseAttributes()

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }

    @Test
    fun `toConciseAttributes should keep map with observedAt for a GeoProperty`() {
        val entity = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "location": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Point",
                        "coordinates": [24.30623, 60.07966]
                    },
                    "observedAt": "2024-01-01T00:00:00Z"
                }
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "location": {
                    "value": {
                        "type": "Point",
                        "coordinates": [24.30623, 60.07966]
                    },
                    "observedAt": "2024-01-01T00:00:00Z"
                }
            }
        """.trimIndent()

        val result = entity.toConciseAttributes()

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }

    @Test
    fun `toConciseAttributes should keep Relationship as map with object key and no type`() {
        val entity = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "isParkedIn": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Parking:01"
                }
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "isParkedIn": {
                    "object": "urn:ngsi-ld:Parking:01"
                }
            }
        """.trimIndent()

        val result = entity.toConciseAttributes()

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }

    @Test
    fun `toConciseAttributes should keep Relationship with observedAt and no type`() {
        val entity = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "isParkedIn": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Parking:01",
                    "observedAt": "2024-01-01T00:00:00Z"
                }
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "isParkedIn": {
                    "object": "urn:ngsi-ld:Parking:01",
                    "observedAt": "2024-01-01T00:00:00Z"
                }
            }
        """.trimIndent()

        val result = entity.toConciseAttributes()

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }

    @Test
    fun `toConciseAttributes should keep JsonProperty as map with json key and no type`() {
        val entity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "metadata": {
                    "type": "JsonProperty",
                    "json": {
                        "key": "value"
                    }
                }
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "metadata": {
                    "json": {
                        "key": "value"
                    }
                }
            }
        """.trimIndent()

        val result = entity.toConciseAttributes()

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }

    @Test
    fun `toConciseAttributes should keep LanguageProperty as map with languageMap key and no type`() {
        val entity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "name": {
                    "type": "LanguageProperty",
                    "languageMap": {
                        "fr": "Tour Eiffel",
                        "en": "Eiffel Tower"
                    }
                }
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "name": {
                    "languageMap": {
                        "fr": "Tour Eiffel",
                        "en": "Eiffel Tower"
                    }
                }
            }
        """.trimIndent()

        val result = entity.toConciseAttributes()

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }

    @Test
    fun `toConciseAttributes should keep VocabProperty as map with vocab key and no type`() {
        val entity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "status": {
                    "type": "VocabProperty",
                    "vocab": "Open"
                }
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "status": {
                    "vocab": "Open"
                }
            }
        """.trimIndent()

        val result = entity.toConciseAttributes()

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }

    @Test
    fun `toConciseAttributes should return a list of concise instances for multi-instance attributes`() {
        val entity = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "speed": [
                    {
                        "type": "Property",
                        "value": 55,
                        "datasetId": "urn:ngsi-ld:Dataset:01"
                    },
                    {
                        "type": "Property",
                        "value": 54.5,
                        "datasetId": "urn:ngsi-ld:Dataset:02"
                    }
                ]
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "speed": [
                    {
                        "value": 55,
                        "datasetId": "urn:ngsi-ld:Dataset:01"
                    },
                    {
                        "value": 54.5,
                        "datasetId": "urn:ngsi-ld:Dataset:02"
                    }
                ]
            }
        """.trimIndent()

        val result = entity.toConciseAttributes()

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }

    @Test
    fun `toConciseAttributes should leave entity core keys untouched`() {
        val entity = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            }
        """.trimIndent()

        val result = entity.toConciseAttributes()

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }

    @Test
    fun `toFinalRepresentation should apply concise transformation when CONCISE is requested`() {
        val inputEntity = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "speed": {
                    "type": "Property",
                    "value": 100
                },
                "isParkedIn": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Parking:01"
                }
            }
        """.trimIndent().deserializeAsMap()

        val expectedConciseRepresentation = """
            {
                "id": "urn:ngsi-ld:Vehicle:01",
                "type": "Vehicle",
                "speed": 100,
                "isParkedIn": {
                    "object": "urn:ngsi-ld:Parking:01"
                }
            }
        """.trimIndent()

        val result = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.JSON,
                AttributeRepresentation.CONCISE,
                includeSysAttrs = false
            )
        )

        assertJsonPayloadsAreEqual(expectedConciseRepresentation, serializeObject(result))
    }
}
