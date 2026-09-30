package com.egm.stellio.search.service.util

import com.egm.stellio.search.service.registration.model.ServiceInformation
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.mapper
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import org.junit.jupiter.api.Test

class ServiceSchemaValidatorTests {
    private val schema = mapper.readTree(
        """
        {
          "type": "object",
          "required": ["brightness"],
          "properties": {"brightness": {"type": "integer", "minimum": 0, "maximum": 255}},
          "additionalProperties": false
        }
        """.trimIndent()
    )

    @Test
    fun `validateInput should preserve separate definitions for schemas sharing an id`() {
        val schemas = listOf("integer", "string").map { type ->
            mapper.readTree(
                """
                {
                  "${'$'}id": "https://example.org/shared",
                  "${'$'}defs": {"value": {"type": "$type"}},
                  "${'$'}ref": "#/${'$'}defs/value"
                }
                """.trimIndent()
            )
        }
        java.util.stream.IntStream.range(0, 20).forEach {
            ServiceSchemaValidator.validateInput(schemas[0], 1).shouldSucceed()
            ServiceSchemaValidator.validateInput(schemas[1], "one").shouldSucceed()
            ServiceSchemaValidator.validateInput(schemas[0], "one").shouldFailWith {
                it is BadRequestDataException
            }
            ServiceSchemaValidator.validateInput(schemas[1], 1).shouldFailWith {
                it is BadRequestDataException
            }
        }
    }

    @Test
    fun `validateInput should enforce the registered regular expression`() {
        val schema = mapper.readTree("""{"type":"string","pattern":"^[A-Z]{3}-[0-9]{4}$"}""")

        ServiceSchemaValidator.validateInput(schema, "ABC-1234").shouldSucceed()
        listOf("abc-1234", "ABC-123", "prefixABC-1234", "ABC-1234suffix").forEach { input ->
            ServiceSchemaValidator.validateInput(schema, input).shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "Service execution input does not conform to the registration schema"
            }
        }
    }

    @Test
    fun `validateInput should enforce required fields types bounds and additional properties`() {
        ServiceSchemaValidator.validateInput(schema, mapOf("brightness" to 125)).shouldSucceed()
        listOf(
            emptyMap(),
            mapOf("brightness" to "125"),
            mapOf("brightness" to 256),
            mapOf("brightness" to -1),
            mapOf("brightness" to 1.5),
            mapOf("brightness" to 1, "extra" to true)
        ).forEach { input ->
            ServiceSchemaValidator.validateInput(schema, input).shouldFailWith { it is BadRequestDataException }
        }
    }

    @Test
    fun `validateInput should resolve local references and validate array items`() {
        val schema = mapper.readTree(
            """
            {
              "${'$'}defs": {"level": {"type": "integer", "minimum": 0}},
              "type": "array", "items": {"${'$'}ref": "#/${'$'}defs/level"}
            }
            """.trimIndent()
        )
        ServiceSchemaValidator.validateInput(schema, listOf(1, 2)).shouldSucceed()
        ServiceSchemaValidator.validateInput(schema, listOf(1, -1)).shouldFailWith {
            it is BadRequestDataException &&
                it.message == "Service execution input does not conform to the registration schema" &&
                it.detail?.contains("1") == true
        }
    }

    @Test
    fun `validateInput should accept or reject every input with boolean schemas`() {
        ServiceSchemaValidator.validateInput(mapper.readTree("true"), 1).shouldSucceed()
        ServiceSchemaValidator.validateInput(mapper.readTree("false"), 1).shouldFailWith {
            it is BadRequestDataException
        }
    }

    @Test
    fun `validate should reject malformed input and output schemas`() {
        listOf("42", "[]", "null", """{"type":"unknown"}""", """{"required":true}""").forEach { json ->
            val invalid = mapper.readTree(json)
            ServiceInformation(name = "test", input = invalid).validate().shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "Invalid service information 'input' schema" && !it.detail.isNullOrBlank()
            }
            ServiceInformation(name = "test", output = invalid).validate().shouldFailWith {
                it is BadRequestDataException &&
                    it.message == "Invalid service information 'output' schema" && !it.detail.isNullOrBlank()
            }
        }
    }

    @Test
    fun `validate should reject an invalid regular expression`() {
        val schema = mapper.readTree("""{"type":"string","pattern":"["}""")
        ServiceInformation(name = "test", input = schema).validate().shouldFailWith {
            it is BadRequestDataException
        }
    }

    @Test
    fun `validate should reject unresolved external references without fetching them`() {
        val schema = mapper.readTree("""{"${'$'}ref":"https://example.invalid/schema.json"}""")
        ServiceInformation(name = "test", input = schema).validate().shouldFailWith {
            it is BadRequestDataException
        }
    }

    @Test
    fun `validateInput should use boolean exclusive minimum for explicit draft four`() {
        val schema = mapper.readTree(
            """{"${'$'}schema":"http://json-schema.org/draft-04/schema#","minimum":5,"exclusiveMinimum":true}"""
        )
        ServiceSchemaValidator.validateInput(schema, 6).shouldSucceed()
        ServiceSchemaValidator.validateInput(schema, 5).shouldFailWith { it is BadRequestDataException }
    }
}
