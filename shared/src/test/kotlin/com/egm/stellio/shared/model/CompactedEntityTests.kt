package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import org.junit.jupiter.api.Test

class CompactedEntityTests {

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
}
