package com.egm.stellio.shared.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.util.isNgsiLdSupportedName

/**
 * Represents a parsed attribute projection expression following the NGSI-LD Attribute Projection Language (4.21).
 *
 * For example, the following expression: `pick=observation{temperature,humidity}` will be parsed as:
 * ```
 *       AttributeProjection(
 *           "observation",
 *           listOf(AttributeProjection("temperature" to null), AttributeProjection("humidity" to null))
 *       )
 * ```
 */
data class AttributeProjection(
    val attributeName: String,
    val nestedProjections: List<AttributeProjection>? = null
) {
    companion object {

        private val separators = setOf(',', '|')
        private const val OPENING_BRACE = '{'
        private const val CLOSING_BRACE = '}'
        private val allowedSpecialChars = setOf(OPENING_BRACE, CLOSING_BRACE).plus(separators)

        private const val ERROR_TITLE = "Error parsing pick or omit parameter"

        /**
         * Parse pick/omit parameters, according to NGSI-LD Attribute Projection Language ABNF grammar
         * as defined in section 4.21 of the NGSI-LD specification.
         */
        fun parsePickOmitParameters(
            pickParam: String?,
            omitParam: String?
        ): Either<APIException, Pair<List<AttributeProjection>, List<AttributeProjection>>> = either {
            val pick = pickParam?.let { pickValue ->
                parseAttributeProjectionExpression(pickValue.replace(" ", "")).bind()
            } ?: emptyList()

            val omit = omitParam?.let { omitValue ->
                parseAttributeProjectionExpression(omitValue.replace(" ", "")).bind()
            } ?: emptyList()

            Pair(pick, omit)
        }

        private fun parseAttributeProjectionExpression(
            paramValue: String
        ): Either<APIException, List<AttributeProjection>> = either {
            if (paramValue.isBlank())
                toBadRequestDataException("Value cannot be empty").left().bind()

            validateInputCharacters(paramValue).bind()

            val projections = mutableListOf<AttributeProjection>()
            var currentIndex = 0

            // Check for leading separator
            if (paramValue[currentIndex] in allowedSpecialChars)
                toBadRequestDataException("Expression cannot start with a brace, comma or pipe").left().bind()

            while (currentIndex < paramValue.length) {
                checkForConsecutiveSeparators(currentIndex, paramValue).bind()

                // Skip separator
                if (paramValue[currentIndex] in separators) currentIndex++

                // End of parsing of the expression
                if (currentIndex >= paramValue.length) {
                    // Check for trailing separator
                    if (paramValue[currentIndex - 1] in separators)
                        toBadRequestDataException("Expression cannot end with a separator").left().bind()
                    break
                }

                val attrNameStart = currentIndex
                // Move until the next separator or opening brace
                while (currentIndex < paramValue.length && paramValue[currentIndex] !in separators.plus(OPENING_BRACE))
                    currentIndex++

                val attrName = paramValue.substring(attrNameStart, currentIndex)
                if (attrName.isEmpty())
                    toBadRequestDataException("Expression contains an empty attribute name").left().bind()

                val (projection, newIndex) = createAttributeProjection(paramValue, attrName, currentIndex).bind()

                currentIndex = newIndex
                projections.add(projection)
            }

            if (projections.isEmpty())
                toBadRequestDataException("Expression must contain at least one valid attribute name").left().bind()

            projections
        }

        private fun createAttributeProjection(
            input: String,
            attrName: String,
            currentIndex: Int,
        ): Either<APIException, Pair<AttributeProjection, Int>> = either {
            if (currentIndex < input.length && input[currentIndex] == OPENING_BRACE) {
                val (nestedProjections, newIndex) = parseNestedProjection(input, currentIndex).bind()
                Pair(AttributeProjection(attrName, nestedProjections), newIndex)
            } else {
                Pair(AttributeProjection(attrName), currentIndex)
            }
        }

        /**
         * Parse a nested projection starting from an opening brace.
         * Returns the parsed projections and the index after the closing brace.
         */
        private fun parseNestedProjection(
            input: String,
            startIndex: Int
        ): Either<APIException, Pair<List<AttributeProjection>, Int>> = either {
            var currentIndex = startIndex + 1 // skip opening '{'
            val nestedProjections = mutableListOf<AttributeProjection>()
            var foundClosingBrace = false

            // Check for leading separator in nested projection
            if (currentIndex < input.length && input[currentIndex] in separators)
                toBadRequestDataException("Expression cannot contain a separator after an opening brace").left().bind()

            while (currentIndex < input.length) {
                checkForConsecutiveSeparators(currentIndex, input).bind()

                // Skip separator
                if (input[currentIndex] in separators) currentIndex++

                // Check for closing brace
                if (input[currentIndex] == CLOSING_BRACE) {
                    currentIndex++ // consume '}'
                    foundClosingBrace = true
                    break
                }

                // Parse nested attribute name
                val attrNameStart = currentIndex
                while (currentIndex < input.length && input[currentIndex] !in allowedSpecialChars)
                    currentIndex++

                val attrName = input.substring(attrNameStart, currentIndex)
                if (attrName.isEmpty())
                    toBadRequestDataException(
                        "Expression contains an empty attribute name in nested projection"
                    ).left().bind()

                val (projection, newIndex) = createAttributeProjection(input, attrName, currentIndex).bind()

                currentIndex = newIndex
                nestedProjections.add(projection)
            }

            if (!foundClosingBrace)
                toBadRequestDataException("Expression contains an unclosed brace").left().bind()

            if (nestedProjections.isEmpty())
                toBadRequestDataException("Expression contains an empty nested projection").left().bind()

            Pair(nestedProjections, currentIndex)
        }

        fun checkForConsecutiveSeparators(currentIndex: Int, input: String): Either<APIException, Unit> =
            if (
                currentIndex < input.length - 1 &&
                input[currentIndex] in separators &&
                input[currentIndex + 1] in separators
            )
                toBadRequestDataException("Expression cannot contain consecutive separators").left()
            else Unit.right()

        /**
         * Validates that the input string contains only valid characters for NGSI-LD attribute projection.
         * Valid characters include characters allowed in attribute names, braces, commas, and pipes.
         */
        private fun validateInputCharacters(input: String): Either<APIException, Unit> {
            val invalidChars = input.filter { char ->
                !char.toString().isNgsiLdSupportedName() && char !in allowedSpecialChars
            }

            return if (invalidChars.isNotEmpty())
                toBadRequestDataException("Invalid characters in the value ($invalidChars)").left()
            else
                Unit.right()
        }

        private fun toBadRequestDataException(detail: String): BadRequestDataException =
            BadRequestDataException(ERROR_TITLE, detail)
    }
}

fun List<AttributeProjection>.getRootAttributes(): Set<String> =
    map { it.attributeName }.toSet()

fun List<AttributeProjection>.removeAttributes(attributes: Set<String>): List<AttributeProjection> =
    filter { it.attributeName !in attributes }

fun List<AttributeProjection>.getAttributesFor(parentAttributeName: String, level: UInt): Set<String> =
    if (level.toInt() == 1)
        filter { it.attributeName == parentAttributeName }
            .flatMap { it.nestedProjections ?: emptyList() }
            .map { it.attributeName }
            .toSet()
    else
        flatMap { it.nestedProjections ?: emptyList() }.getAttributesFor(parentAttributeName, level - 1.toUInt())
