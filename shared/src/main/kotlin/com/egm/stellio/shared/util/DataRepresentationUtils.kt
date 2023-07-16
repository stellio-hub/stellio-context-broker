package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import arrow.fx.coroutines.parMap
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonUtils.getAllKeys
import com.egm.stellio.shared.util.JsonUtils.getAllValues

/**
 * Checks whether the given JSON-LD object contains Type Names, Property Names and Relationship Names
 * that conforms to the restrictions defined in 4.6.2
 */
suspend fun Map<String, Any>.checkNamesAreNgsiLdSupported(): Either<APIException, Map<String, Any>> = either {
    val keys = filter { it.key != JsonLdUtils.JSONLD_CONTEXT }.getAllKeys()
    keys.parMap { key -> key.checkNameIsNgsiLdSupported().bind() }
    when (val type = get(JsonLdUtils.JSONLD_TYPE_TERM)) {
        is String -> type.checkNameIsNgsiLdSupported().bind()
        is List<*> -> (type as List<String>).parMap { it.checkNameIsNgsiLdSupported().bind() }.map { Unit }
        else -> Unit.right().bind()
    }
    this@checkNamesAreNgsiLdSupported
}

suspend fun List<Map<String, Any>>.checkNamesAreNgsiLdSupported(): Either<APIException, List<Map<String, Any>>> =
    either {
        parMap {
            it.checkNamesAreNgsiLdSupported().bind()
        }
    }

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
suspend fun Map<String, Any>.checkContentIsNgsiLdSupported(): Either<APIException, Map<String, Any>> = either {
    val values = filter { it.key != JsonLdUtils.JSONLD_CONTEXT }.getAllValues()
    values.parMap { value ->
        (value?.checkContentIsNgsiLdSupported() ?: BadRequestDataException(NULL_VALUE_IN_CONTENT).left()).bind()
    }
    this@checkContentIsNgsiLdSupported
}

suspend fun List<Map<String, Any>>.checkContentIsNgsiLdSupported(): Either<APIException, List<Map<String, Any>>> =
    either {
        parMap {
            it.checkContentIsNgsiLdSupported().bind()
        }
    }

private fun Any.checkContentIsNgsiLdSupported(): Either<APIException, Unit> =
    if (this is String) {
        if (this.isNgsiLdSupportedContent()) Unit.right()
        else BadRequestDataException(invalidCharacterInContent(this)).left()
    } else Unit.right()

/**
 * Relaxed list of forbidden characters in entity content defined in 4.6.4. Full list prevents from a lot of use-cases.
 */
private val invalidCharactersForValues = ">\";".toCharArray()

/**
 * Returns whether the given string is a supported content as defined in 4.6.3
 */
private fun String.isNgsiLdSupportedContent(): Boolean =
    this.indexOfAny(invalidCharactersForValues) == -1
