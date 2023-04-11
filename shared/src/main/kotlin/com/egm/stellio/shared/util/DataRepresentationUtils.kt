package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import arrow.fx.coroutines.parTraverseEither
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonUtils.getAllKeys
import com.egm.stellio.shared.util.JsonUtils.getAllValues

/**
 * Checks whether the given JSON-LD object contains Type Names, Property Names and Relationship Names
 * that conforms to the restrictions defined in 4.6.2
 */
suspend fun Map<String, Any>.checkNamesAreNgsiLdSupported(): Either<APIException, Map<String, Any>> =
    this.filter { it.key != JsonLdUtils.JSONLD_CONTEXT }
        .getAllKeys()
        .parTraverseEither { key -> key.checkNameIsNgsiLdSupported() }
        .flatMap {
            when (val type = this[JsonLdUtils.JSONLD_TYPE_TERM]) {
                is String -> type.checkNameIsNgsiLdSupported()
                is List<*> -> (type as List<String>).parTraverseEither { it.checkNameIsNgsiLdSupported() }.map { Unit }
                else -> Unit.right()
            }
        }
        .map { this }

suspend fun List<Map<String, Any>>.checkNamesAreNgsiLdSupported(): Either<APIException, List<Map<String, Any>>> =
    this.parTraverseEither { it.checkNamesAreNgsiLdSupported() }

fun String.checkNameIsNgsiLdSupported(): Either<APIException, Unit> =
    if (this.isNgsiLdSupportedName()) Unit.right()
    else BadRequestDataException(invalidCharacterInName(this)).left()

/**
 * Returns whether the given string is a supported name as defined in 4.6.2
 */
private fun String.isNgsiLdSupportedName(): Boolean =
    this.all { char -> char.isLetterOrDigit() || listOf(':', '_').contains(char) }

/**
 * Checks whether the given JSON-LD object contains content that conforms to the restrictions defined in 4.6.3
 */
suspend fun Map<String, Any>.checkContentIsNgsiLdSupported(): Either<APIException, Map<String, Any>> =
    this.filter { it.key != JsonLdUtils.JSONLD_CONTEXT }
        .getAllValues()
        .parTraverseEither { value ->
            value?.checkContentIsNgsiLdSupported() ?: BadRequestDataException(NULL_VALUE_IN_CONTENT).left()
        }
        .map { this }

suspend fun List<Map<String, Any>>.checkContentIsNgsiLdSupported(): Either<APIException, List<Map<String, Any>>> =
    this.parTraverseEither { it.checkContentIsNgsiLdSupported() }

private fun Any.checkContentIsNgsiLdSupported(): Either<APIException, Unit> =
    if (this is String) {
        if (this.isNgsiLdSupportedContent()) Unit.right()
        else BadRequestDataException(invalidCharacterInContent(this)).left()
    } else Unit.right()

private val invalidCharactersForValues = "<>\"'=()".toCharArray()

/**
 * Returns whether the given string is a supported content as defined in 4.6.3
 */
private fun String.isNgsiLdSupportedContent(): Boolean =
    this.indexOfAny(invalidCharactersForValues) == -1
