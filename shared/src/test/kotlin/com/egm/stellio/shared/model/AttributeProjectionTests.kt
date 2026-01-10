package com.egm.stellio.shared.model

import com.egm.stellio.shared.model.AttributeProjection.Companion.parsePickOmitParameters
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceedAndResult
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class AttributeProjectionTests {

    @Test
    fun `it should return empty sets when no pick or omit parameters are provided`() {
        val (pick, omit) = parsePickOmitParameters(null, null).shouldSucceedAndResult()

        assertThat(pick).isEmpty()
        assertThat(omit).isEmpty()
    }

    @Test
    fun `it should reject empty pick parameter`() {
        parsePickOmitParameters("", null).shouldFailWith {
            it is BadRequestDataException &&
                it.detail == "Value cannot be empty"
        }
    }

    @Test
    fun `it should reject invalid attribute names`() {
        parsePickOmitParameters("invalid%,temperature", null).shouldFailWith {
            it is BadRequestDataException &&
                it.detail == "Invalid characters in the value (%)"
        }
    }

    @Test
    fun `it should reject invalid attribute projection expression`() {
        parsePickOmitParameters("servesDataset{title", null).shouldFailWith {
            it is BadRequestDataException &&
                it.detail == "Expression contains an unclosed brace"
        }
    }

    @Test
    fun `it should reject invalid attribute projection expression missing a sub-parameter`() {
        parsePickOmitParameters("servesDataset{}", null).shouldFailWith {
            it is BadRequestDataException &&
                it.detail == "Expression contains an empty nested projection"
        }
    }

    @Test
    fun `it should parse pick or omit parameters only including first level attributes`() {
        val (pick, omit) = parsePickOmitParameters("temperature,humidity", "title,description")
            .shouldSucceedAndResult()

        Assertions.assertThat(pick)
            .hasSize(2)
            .containsExactly(
                AttributeProjection("temperature", null),
                AttributeProjection("humidity", null)
            )
        Assertions.assertThat(omit)
            .hasSize(2)
            .containsExactly(
                AttributeProjection("title", null),
                AttributeProjection("description", null)
            )
    }

    @Test
    fun `it should parse a pick parameter including attributes of a first level relationship`() {
        val (pick, omit) = parsePickOmitParameters("temperature,servesDataset{title,description}", null)
            .shouldSucceedAndResult()

        Assertions.assertThat(pick)
            .hasSize(2)
            .containsExactly(
                AttributeProjection("temperature", null),
                AttributeProjection(
                    "servesDataset",
                    listOf(
                        AttributeProjection("title", null),
                        AttributeProjection("description", null)
                    )
                )
            )
        Assertions.assertThat(omit).isEmpty()
    }

    @Test
    fun `it should parse a pick parameter including attributes of a second level relationship`() {
        val (pick, _) = parsePickOmitParameters(
            "temperature,servesDataset{title,description,catalog{publisher}}",
            null
        ).shouldSucceedAndResult()

        Assertions.assertThat(pick)
            .hasSize(2)
            .containsExactly(
                AttributeProjection("temperature", null),
                AttributeProjection(
                    "servesDataset",
                    listOf(
                        AttributeProjection("title", null),
                        AttributeProjection("description", null),
                        AttributeProjection(
                            "catalog",
                            listOf(
                                AttributeProjection("publisher", null)
                            )
                        )
                    )
                )
            )
    }

    @Test
    fun `it should ignore whitespaces in pick or omit parameters`() {
        val (pick, _) = parsePickOmitParameters("temperature, humidity, servesDataset{ title, description}", null)
            .shouldSucceedAndResult()

        Assertions.assertThat(pick)
            .hasSize(3)
            .containsExactly(
                AttributeProjection("temperature", null),
                AttributeProjection("humidity", null),
                AttributeProjection(
                    "servesDataset",
                    listOf(
                        AttributeProjection("title", null),
                        AttributeProjection("description", null)
                    )
                )
            )
    }

    @Test
    fun `it should get attributes at the root level`() {
        val (pick, _) = parsePickOmitParameters("temperature,humidity,servesDataset{title}", null)
            .shouldSucceedAndResult()

        val rootAttributes = pick.getRootAttributesToPick()

        assertThat(rootAttributes)
            .hasSize(3)
            .containsExactlyInAnyOrder("temperature", "humidity", "servesDataset")
    }

    @Test
    fun `it should get attributes at any level in the graph`() {
        val (pick, _) = parsePickOmitParameters(
            "servesDataset{title,description,catalog{publisher}},belongsTo{name,owner,servesDatatset{location}}",
            null
        ).shouldSucceedAndResult()

        assertThat(
            pick.getAttributesFor("servesDataset", 1.toUInt())
        ).hasSize(3)
            .containsExactlyInAnyOrder("title", "description", "catalog")

        assertThat(
            pick.getAttributesFor("catalog", 2.toUInt())
        ).hasSize(1)
            .containsExactlyInAnyOrder("publisher")

        assertThat(
            pick.getAttributesFor("servesDatatset", 2.toUInt())
        ).hasSize(1)
            .containsExactlyInAnyOrder("location")
    }

    @Test
    fun `it should handle pipe separator in addition to comma`() {
        val (pick, _) = parsePickOmitParameters("temperature|humidity|servesDataset{title|description}", null)
            .shouldSucceedAndResult()

        Assertions.assertThat(pick)
            .hasSize(3)
            .containsExactly(
                AttributeProjection("temperature", null),
                AttributeProjection("humidity", null),
                AttributeProjection(
                    "servesDataset",
                    listOf(
                        AttributeProjection("title", null),
                        AttributeProjection("description", null)
                    )
                )
            )
    }

    @Test
    fun `it should handle mixed separators`() {
        val (pick, _) = parsePickOmitParameters("temperature,humidity|servesDataset{title,description}", null)
            .shouldSucceedAndResult()

        Assertions.assertThat(pick)
            .hasSize(3)
            .containsExactly(
                AttributeProjection("temperature", null),
                AttributeProjection("humidity", null),
                AttributeProjection(
                    "servesDataset",
                    listOf(
                        AttributeProjection("title", null),
                        AttributeProjection("description", null)
                    )
                )
            )
    }

    @Test
    fun `it should reject multiple consecutive separators`() {
        parsePickOmitParameters("temperature,,humidity", null).shouldFailWith {
            it is BadRequestDataException &&
                it.detail == "Expression cannot contain consecutive separators"
        }
    }

    @Test
    fun `it should reject leading separators`() {
        parsePickOmitParameters(",temperature,humidity", null).shouldFailWith {
            it is BadRequestDataException &&
                it.detail == "Expression cannot start with a brace, comma or pipe"
        }
    }

    @Test
    fun `it should reject trailing separators`() {
        parsePickOmitParameters("temperature,humidity,", null).shouldFailWith {
            it is BadRequestDataException &&
                it.detail == "Expression cannot end with a separator"
        }
    }

    @Test
    fun `it should reject unclosed nested braces`() {
        parsePickOmitParameters("observation{temperature,observedAt{location}", null).shouldFailWith {
            it is BadRequestDataException &&
                it.detail == "Expression contains an unclosed brace"
        }
    }

    @Test
    fun `it should reject a separator immediately following an opening brace`() {
        parsePickOmitParameters("observation{,temperature}", null).shouldFailWith {
            it is BadRequestDataException &&
                it.detail == "Expression cannot contain a separator after an opening brace"
        }
    }

    @Test
    fun `it should reject empty attribute names in nested projections`() {
        parsePickOmitParameters("observation{temperature,,humidity}", null).shouldFailWith {
            it is BadRequestDataException &&
                it.detail == "Expression cannot contain consecutive separators"
        }
    }

    @Test
    fun `it should handle deeply nested projections`() {
        val (pick, _) = parsePickOmitParameters(
            "observation{temperature,observedAt{location,accuracy}},humidity",
            null
        ).shouldSucceedAndResult()

        Assertions.assertThat(pick)
            .hasSize(2)
            .containsExactly(
                AttributeProjection(
                    "observation",
                    listOf(
                        AttributeProjection("temperature", null),
                        AttributeProjection(
                            "observedAt",
                            listOf(
                                AttributeProjection("location", null),
                                AttributeProjection("accuracy", null)
                            )
                        )
                    )
                ),
                AttributeProjection("humidity", null)
            )
    }

    @Test
    fun `it should handle complex real-world examples`() {
        val (pick, _) = parsePickOmitParameters(
            """
                temperature,humidity,pressure,
                servesDataset{title,description,publisher,catalog{name,version}},
                belongsTo{name,owner}
            """.trimIndent().replace("\n", ""),
            null
        ).shouldSucceedAndResult()

        assertThat(pick).hasSize(5)
        assertThat(pick.getRootAttributesToPick())
            .containsExactlyInAnyOrder("temperature", "humidity", "pressure", "servesDataset", "belongsTo")
        assertThat(pick.getAttributesFor("servesDataset", 1.toUInt()))
            .containsExactlyInAnyOrder("title", "description", "publisher", "catalog")
        assertThat(pick.getAttributesFor("catalog", 2.toUInt()))
            .containsExactlyInAnyOrder("name", "version")
        assertThat(pick.getAttributesFor("belongsTo", 1.toUInt()))
            .containsExactlyInAnyOrder("name", "owner")
    }
}
