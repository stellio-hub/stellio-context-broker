package com.egm.stellio.search.service.registration.model

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecution.invalidInputElementKeyMessage
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecution.invalidServiceExecutionInputMessage
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecution.missingServiceExecutionInputMessage
import com.fasterxml.jackson.annotation.JsonProperty
import java.math.BigDecimal

data class InputInformation(
    val type: InputInformationType,
    val required: Boolean = false,
    val properties: Map<String, InputInformation>? = null,
    val elements: Map<String, InputInformation>? = null,
    val minimum: BigDecimal? = null,
    val maximum: BigDecimal? = null,
    val matchRegex: String? = null,
    val maxSize: Int? = null
) {

    fun checkValue(value: Any?, path: String = "input"): Either<APIException, Unit> = either {
        ensure(value != null || !required) {
            BadRequestDataException(missingServiceExecutionInputMessage(path))
        }
        if (value == null) return@either

        ensure(matchesType(value)) {
            BadRequestDataException(invalidServiceExecutionInputMessage(path, type.name.lowercase()))
        }

        when (type) {
            InputInformationType.OBJECT -> validateObject(value as Map<*, *>, path).bind()
            InputInformationType.ARRAY -> validateArray(value as List<*>, path).bind()
            InputInformationType.STRING -> validateString(value as String, path).bind()
            InputInformationType.NUMBER,
            InputInformationType.INTEGER -> validateRange(value as Number, path).bind()

            else -> Unit
        }
    }

    private fun validateObject(value: Map<*, *>, path: String): Either<APIException, Unit> = either {
        properties.orEmpty().forEach { (propertyName, propertyInformation) ->
            val propertyPath = "$path.$propertyName"
            propertyInformation.checkValue(
                value.getOrDefault(propertyName, null),
                propertyPath
            ).bind()
        }
    }

    private fun validateArray(value: List<*>, path: String): Either<APIException, Unit> = either {
        validateMaxSize(value.size, path).bind()
        elements.orEmpty().forEach { (key, elementInformation) ->
            if (key != "*") {
                val index = key.toIntOrNull()
                ensure(index != null && index >= 0) {
                    BadRequestDataException(invalidInputElementKeyMessage(key))
                }
                if (index >= value.size) {
                    elementInformation.checkValue(null, "$path[$index]").bind()
                }
            }
        }

        value.forEachIndexed { index, element ->
            (elements?.get(index.toString()) ?: elements?.get("*"))
                ?.checkValue(element, "$path[$index]")
                ?.bind()
        }
    }

    private fun validateRange(value: Number, path: String): Either<APIException, Unit> = either {
        val decimalValue = value.toString().toBigDecimal()
        minimum?.let {
            ensure(decimalValue >= it) {
                BadRequestDataException(
                    invalidServiceExecutionInputMessage(path, "a number greater than or equal to $it")
                )
            }
        }
        maximum?.let {
            ensure(decimalValue <= it) {
                BadRequestDataException(
                    invalidServiceExecutionInputMessage(path, "a number less than or equal to $it")
                )
            }
        }
    }

    private fun validateString(value: String, path: String): Either<APIException, Unit> = either {
        matchRegex?.let { pattern ->
            val regex = runCatching { Regex(pattern) }.getOrNull()
            ensure(regex?.matches(value) == true) {
                BadRequestDataException(
                    invalidServiceExecutionInputMessage(path, "a string matching regex '$pattern'")
                )
            }
        }
        validateMaxSize(value.length, path).bind()
    }

    private fun validateMaxSize(size: Int, path: String): Either<APIException, Unit> = either {
        maxSize?.let {
            ensure(size <= it) {
                BadRequestDataException(
                    invalidServiceExecutionInputMessage(path, "a value with a maximum size of $it")
                )
            }
        }
    }

    private fun matchesType(value: Any?): Boolean =
        when (type) {
            InputInformationType.OBJECT -> value is Map<*, *>
            InputInformationType.ARRAY -> value is List<*>
            InputInformationType.STRING -> value is String
            InputInformationType.NUMBER -> value is Number
            InputInformationType.INTEGER -> value is Number && value.isInteger()
            InputInformationType.BOOLEAN -> value is Boolean
            InputInformationType.NULL -> value == null
        }

    private fun Number.isInteger(): Boolean =
        toString().toBigDecimalOrNull()
            ?.stripTrailingZeros()
            ?.scale()
            ?.let { it <= 0 }
            ?: false
}

enum class InputInformationType {
    @JsonProperty("object")
    OBJECT,

    @JsonProperty("array")
    ARRAY,

    @JsonProperty("string")
    STRING,

    @JsonProperty("number")
    NUMBER,

    @JsonProperty("integer")
    INTEGER,

    @JsonProperty("boolean")
    BOOLEAN,

    @JsonProperty("null")
    NULL
}
