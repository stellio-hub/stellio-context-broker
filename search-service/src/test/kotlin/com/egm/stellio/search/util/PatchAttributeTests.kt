package com.egm.stellio.search.util

import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXTS
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class PatchAttributeTests {

    companion object {

        @JvmStatic
        fun partialUpdatePatchProvider(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(
                    """
                    {
                        "attribute": {
                            "type": "Property",
                            "value": 12.0,
                            "observedAt": "2024-04-14T12:34:56Z"
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "Property",
                            "value": 12.2,
                            "unitCode": "GRM"
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "Property",
                            "value": 12.2,
                            "unitCode": "GRM",
                            "observedAt": "2024-04-14T12:34:56Z"
                        }
                    }
                    """.trimIndent()
                ),
                Arguments.of(
                    """
                    {
                        "incoming": {
                            "type": "JsonProperty",
                            "json": { "id": 1, "b": null, "c": 12.4 },
                            "observedAt": "2022-12-24T14:01:22.066Z",
                            "subAttribute": {
                                "type": "Property",
                                "value": "subAttribute"
                            }
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "incoming": {
                            "type": "JsonProperty",
                            "json": { "id": 2, "b": "something" },
                            "observedAt": "2023-12-24T14:01:22.066Z"
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "incoming": {
                            "type": "JsonProperty",
                            "json": { "id": 2, "b": "something" },
                            "observedAt": "2023-12-24T14:01:22.066Z",
                            "subAttribute": {
                                "type": "Property",
                                "value": "subAttribute"
                            }
                        }
                    }
                    """.trimIndent()
                ),
                Arguments.of(
                    """
                    {
                        "attribute": {
                            "type": "VocabProperty",
                            "vocab": "stellio"
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "VocabProperty",
                            "vocab": "egm"
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "VocabProperty",
                            "vocab": "egm"
                        }
                    }
                    """.trimIndent()
                )
            )
        }

        @JvmStatic
        fun mergePatchProvider(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(
                    """
                    {
                        "attribute": {
                            "type": "Property",
                            "value": 12.0,
                            "observedAt": "2024-04-14T12:34:56Z",
                            "subAttribute": {
                                "type": "Property",
                                "value": "subAttribute"
                            }
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "Property",
                            "value": 12.2,
                            "unitCode": "GRM",
                            "subAttribute": {
                                "type": "Property",
                                "value": "newSubAttributeValue"
                            }
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "Property",
                            "value": 12.2,
                            "unitCode": "GRM",
                            "observedAt": "2024-04-14T12:34:56Z",
                            "subAttribute": {
                                "type": "Property",
                                "value": "newSubAttributeValue"
                            }
                        }
                    }
                    """.trimIndent()
                ),
                Arguments.of(
                    """
                    {
                        "attribute": {
                            "type": "Property",
                            "value": { "en": "car", "fr": "voiture" }
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "Property",
                            "value": { "fr": "vélo", "es": "bicicleta" }
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "Property",
                            "value": { "en": "car", "fr": "vélo", "es": "bicicleta" }
                        }
                    }
                    """.trimIndent()
                ),
                Arguments.of(
                    """
                    {
                        "attribute": {
                            "type": "Property",
                            "value": [ "car", "voiture" ]
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "Property",
                            "value": [ "vélo", "bicicleta" ]
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "Property",
                            "value": [ "vélo", "bicicleta" ]
                        }
                    }
                    """.trimIndent()
                ),
                Arguments.of(
                    """
                    {
                        "attribute": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:Entity:01"
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:Entity:02"
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:Entity:02"
                        }
                    }
                    """.trimIndent()
                ),
                Arguments.of(
                    """
                    {
                        "attribute": {
                            "type": "LanguageProperty",
                            "languageMap": { "en": "train", "fr": "train" }
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "LanguageProperty",
                            "languageMap": { "fr": "TGV", "es": "tren" }
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "LanguageProperty",
                            "languageMap": { "en": "train", "fr": "TGV", "es": "tren" }
                        }
                    }
                    """.trimIndent()
                ),
                Arguments.of(
                    """
                    {
                        "incoming": {
                            "type": "JsonProperty",
                            "json": { "a": 1, "b": "thing" }
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "incoming": {
                            "type": "JsonProperty",
                            "json": { "a": 2, "c": "other thing" }
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "incoming": {
                            "type": "JsonProperty",
                            "json": { "a": 2, "b": "thing", "c": "other thing" }
                        }
                    }
                    """.trimIndent()
                ),
                Arguments.of(
                    """
                    {
                        "attribute": {
                            "type": "VocabProperty",
                            "vocab": "stellio"
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "VocabProperty",
                            "vocab": "egm"
                        }
                    }
                    """.trimIndent(),
                    """
                    {
                        "attribute": {
                            "type": "VocabProperty",
                            "vocab": "egm"
                        }
                    }
                    """.trimIndent()
                )
            )
        }
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.util.PatchAttributeTests#partialUpdatePatchProvider")
    fun `it should apply a partial update patch behavior to attribute instance`(
        source: String,
        target: String,
        expected: String
    ) = runTest {
        val (mergeResult, _) = partialUpdatePatch(
            expandAttribute(source, NGSILD_TEST_CORE_CONTEXTS).second[0],
            expandAttribute(target, NGSILD_TEST_CORE_CONTEXTS).second[0]
        )

        assertJsonPayloadsAreEqual(
            serializeObject(expandAttribute(expected, NGSILD_TEST_CORE_CONTEXTS).second[0]),
            mergeResult
        )
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.util.PatchAttributeTests#mergePatchProvider")
    fun `it should apply a merge patch behavior to attribute instance`(
        source: String,
        target: String,
        expected: String
    ) = runTest {
        val (mergeResult, _) = mergePatch(
            expandAttribute(source, NGSILD_TEST_CORE_CONTEXTS).second[0],
            expandAttribute(target, NGSILD_TEST_CORE_CONTEXTS).second[0]
        )

        assertJsonPayloadsAreEqual(
            serializeObject(expandAttribute(expected, NGSILD_TEST_CORE_CONTEXTS).second[0]),
            mergeResult
        )
    }
}
