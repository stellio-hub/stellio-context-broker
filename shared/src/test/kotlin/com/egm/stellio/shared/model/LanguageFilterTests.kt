package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class LanguageFilterTests {

    companion object {

        @JvmStatic
        fun normalizedResultsProvider(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(
                    "nl",
                    """
                    "languageProperty": {
                        "type": "Property",
                        "value": "Grote Markt",
                        "lang": "nl"
                    }
                    """.trimIndent()
                ),
                Arguments.of(
                    "en",
                    """
                    "languageProperty": {
                        "type": "Property",
                        "value": "Big Place",
                        "lang": "@none"
                    }
                    """.trimIndent()
                ),
                Arguments.of(
                    "*",
                    """
                    "languageProperty": {
                        "type": "Property",
                        "value": "Big Place",
                        "lang": "@none"
                    }
                    """.trimIndent()
                ),
                Arguments.of(
                    "fr-CH,fr;q=0.9,en;q=0.8,*;q=0.5",
                    """
                    "languageProperty": {
                        "type": "Property",
                        "value": "Grand Place",
                        "lang": "fr"
                    }
                    """.trimIndent()
                )
            )
        }

        @JvmStatic
        fun simplifiedResultsProvider(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(
                    "nl",
                    """
                    "languageProperty": "Grote Markt"
                    """.trimIndent()
                ),
                Arguments.of(
                    "en",
                    """
                    "languageProperty": "Grand Place"
                    """.trimIndent()
                ),
                Arguments.of(
                    "*",
                    """
                    "languageProperty": "Grand Place"
                    """.trimIndent()
                ),
                Arguments.of(
                    "fr-CH,fr;q=0.9,en;q=0.8,*;q=0.5",
                    """
                    "languageProperty": "Grand Place"
                    """.trimIndent()
                )
            )
        }
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.shared.model.LanguageFilterTests#normalizedResultsProvider")
    fun `it should return the normalized representation of a LanguageProperty with a language filter`(
        languageFilter: String,
        expectedAttribute: String
    ) {
        val compactedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "languageProperty": {
                    "type": "LanguageProperty",
                    "languageMap": {
                        "fr": "Grand Place",
                        "nl": "Grote Markt",
                        "@none": "Big Place"
                    }
                }
            }
        """.trimIndent()
            .deserializeAsMap()

        val filteredRepresentation = compactedEntity.toFilteredLanguageProperties(languageFilter)

        val expectedFilteredRepresentation = """
            {
               "id": "urn:ngsi-ld:Entity:01",
               "type": "Entity",
               $expectedAttribute
            }
        """.trimIndent()

        assertJsonPayloadsAreEqual(expectedFilteredRepresentation, serializeObject(filteredRepresentation))
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.shared.model.LanguageFilterTests#simplifiedResultsProvider")
    fun `it should return the simplfied representation of a LanguageProperty with a language filter`(
        languageFilter: String,
        expectedAttribute: String
    ) {
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
        """.trimIndent()
            .deserializeAsMap()

        val filteredRepresentation = compactedEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.JSON,
                AttributeRepresentation.SIMPLIFIED,
                false,
                languageFilter
            )
        )

        val expectedFilteredRepresentation = """
            {
               "id": "urn:ngsi-ld:Entity:01",
               "type": "Entity",
               $expectedAttribute
            }
        """.trimIndent()

        assertJsonPayloadsAreEqual(expectedFilteredRepresentation, serializeObject(filteredRepresentation))
    }

    @Test
    fun `it should filter language properties for sub attributes`() = runTest {
        val entity = """
            {
                "id": "urn:ngsi-ld:Beehive:01",
                "type": "Beehive",
                "managedBy": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Beekeeper:1",
                    "name": {
                        "type": "LanguageProperty",
                        "languageMap": {
                            "en": "beekeeper",
                            "fr": "apiculteur"
                        }
                    }
                }
            }
        """.trimIndent().deserializeAsMap()

        val filteredEntity = entity.toFilteredLanguageProperties("en")

        assertJsonPayloadsAreEqual(
            """
                {
                    "id": "urn:ngsi-ld:Beehive:01",
                    "type": "Beehive",
                    "managedBy": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:Beekeeper:1",
                        "name": {
                            "type": "Property",
                            "value": "beekeeper",
                            "lang": "en"
                        }
                    }
                }
            """.trimIndent(),
            serializeObject(filteredEntity)
        )
    }

    @Test
    fun `it should filter language properties for an attribute and its sub attributes`() = runTest {
        val entity = """
            {
                "id": "urn:ngsi-ld:Beehive:01",
                "type": "Beehive",
                "name": {
                    "type": "LanguageProperty",
                    "languageMap": {
                        "en": "beekeeper",
                        "fr": "apiculteur"
                    },
                    "subAttribute": {
                        "type": "LanguageProperty",
                        "languageMap": {
                            "en": "English",
                            "fr": "Français"
                        }
                    }
                }
            }
        """.trimIndent().deserializeAsMap()

        val filteredEntity = entity.toFilteredLanguageProperties("en")

        assertJsonPayloadsAreEqual(
            """
             {
                 "id": "urn:ngsi-ld:Beehive:01",
                 "type": "Beehive",
                 "name": {
                     "type": "Property",
                     "value": "beekeeper",
                     "lang": "en",
                     "subAttribute": {
                         "type": "Property",
                         "value": "English",
                         "lang": "en"
                     }
                 }
             }
            """.trimIndent(),
            serializeObject(filteredEntity)
        )
    }
}
