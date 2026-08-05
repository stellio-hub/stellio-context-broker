package com.egm.stellio.search.service.registration.model

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class InputInformationTests {
    @Test
    fun `checkValue should apply required to the current information`() = runTest {
        InputInformation(
            type = InputInformationType.STRING,
            required = false
        ).checkValue(null).shouldSucceed()

        InputInformation(
            type = InputInformationType.STRING,
            required = true
        ).checkValue(null).shouldFailWith {
            it is BadRequestDataException && it.message.contains("required value 'input'")
        }
    }

    @Test
    fun `checkValue should apply exact and fallback array element schemas`() = runTest {
        val inputInformation = InputInformation(
            type = InputInformationType.ARRAY,
            elements = mapOf(
                "0" to InputInformation(
                    type = InputInformationType.STRING,
                    required = true
                ),
                "1" to InputInformation(type = InputInformationType.BOOLEAN),
                "*" to InputInformation(type = InputInformationType.INTEGER)
            )
        )

        inputInformation.checkValue(listOf("first", true, 2)).shouldSucceed()
        inputInformation.checkValue(listOf("first")).shouldSucceed()
        inputInformation.checkValue(listOf("first", true, "invalid")).shouldFailWith {
            it is BadRequestDataException && it.message.contains("input[2]")
        }
        inputInformation.checkValue(emptyList<Any>()).shouldFailWith {
            it is BadRequestDataException && it.message.contains("input[0]")
        }
    }

    @Test
    fun `checkValue should check required object properties and number range`() = runTest {
        val inputInformation = InputInformation(
            type = InputInformationType.OBJECT,
            properties = mapOf(
                "brightness" to InputInformation(
                    type = InputInformationType.INTEGER,
                    required = true,
                    minimum = 0.toBigDecimal(),
                    maximum = 255.toBigDecimal()
                )
            )
        )

        inputInformation.checkValue(mapOf("brightness" to 125)).shouldSucceed()
        inputInformation.checkValue(emptyMap<String, Any>()).shouldFailWith {
            it is BadRequestDataException && it.message.contains("input.brightness")
        }
        inputInformation.checkValue(mapOf("brightness" to 256)).shouldFailWith {
            it is BadRequestDataException && it.message.contains("less than or equal to 255")
        }
    }

    @Test
    fun `checkValue should check decimal number range`() = runTest {
        val inputInformation = InputInformation(
            type = InputInformationType.NUMBER,
            minimum = 0.5.toBigDecimal(),
            maximum = 2.5.toBigDecimal()
        )

        inputInformation.checkValue(1.5).shouldSucceed()
        inputInformation.checkValue(0.25).shouldFailWith {
            it is BadRequestDataException && it.message.contains("greater than or equal to 0.5")
        }
        inputInformation.checkValue(2.75).shouldFailWith {
            it is BadRequestDataException && it.message.contains("less than or equal to 2.5")
        }
    }

    @Test
    fun `checkValue should check string regex and maximum size`() = runTest {
        val inputInformation = InputInformation(
            type = InputInformationType.STRING,
            matchRegex = "[A-Z0-9]+",
            maxSize = 5
        )

        inputInformation.checkValue("AB123").shouldSucceed()
        inputInformation.checkValue("ab123").shouldFailWith {
            it is BadRequestDataException && it.message.contains("matching regex")
        }
        inputInformation.checkValue("ABC123").shouldFailWith {
            it is BadRequestDataException && it.message.contains("maximum size of 5")
        }
    }

    @Test
    fun `checkValue should check array maximum size`() = runTest {
        val inputInformation = InputInformation(
            type = InputInformationType.ARRAY,
            elements = mapOf("*" to InputInformation(type = InputInformationType.INTEGER)),
            maxSize = 2
        )

        inputInformation.checkValue(listOf(1, 2)).shouldSucceed()
        inputInformation.checkValue(listOf(1, 2, 3)).shouldFailWith {
            it is BadRequestDataException && it.message.contains("maximum size of 2")
        }
    }
}
