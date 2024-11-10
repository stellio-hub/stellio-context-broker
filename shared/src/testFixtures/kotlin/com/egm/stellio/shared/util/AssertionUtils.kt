package com.egm.stellio.shared.util

import arrow.core.Either
import com.egm.stellio.shared.model.APIException
import com.fasterxml.jackson.core.filter.FilteringParserDelegate
import com.fasterxml.jackson.core.filter.TokenFilter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import java.io.ByteArrayInputStream

fun assertJsonPayloadsAreEqual(expectation: String, actual: String, ignoredKeys: Set<String> = emptySet()) {
    val tokenFilter: TokenFilter = object : TokenFilter() {
        override fun includeProperty(name: String): TokenFilter? =
            if (ignoredKeys.contains(name)) null
            else INCLUDE_ALL
    }

    val filteredExpectation = FilteringParserDelegate(
        mapper.createParser(ByteArrayInputStream(expectation.toByteArray())),
        tokenFilter,
        TokenFilter.Inclusion.INCLUDE_ALL_AND_PATH,
        true
    )

    val filteredActual = FilteringParserDelegate(
        mapper.createParser(ByteArrayInputStream(actual.toByteArray())),
        tokenFilter,
        TokenFilter.Inclusion.INCLUDE_ALL_AND_PATH,
        true
    )

    assertEquals(mapper.readTree(filteredExpectation), mapper.readTree(filteredActual))
}

fun <T> Either<APIException, T>.shouldSucceedWith(assertions: (T) -> Unit) =
    fold({
        fail("it should have not returned an error $it")
    }, {
        assertions(it)
    })

fun Either<APIException, Any>.shouldSucceed() =
    fold({
        fail("it should have not returned an error $it")
    }, {})

fun <T> Either<APIException, T>.shouldSucceedAndResult(): T =
    fold({
        fail("it should have not returned an error $it")
    }, { it })

fun <T> Either<APIException, T>.shouldFail(assertions: (APIException) -> Unit) =
    fold({
        assertions(it)
    }, {
        fail("it should have returned a left exception")
    })

fun <T> Either<APIException, T>.shouldFailWith(assertions: (APIException) -> Boolean) =
    fold({
        assertTrue(assertions(it))
    }, {
        fail("it should have returned a left exception")
    })

fun assertEqualsIgnoringNoise(expected: String, actual: String) =
    assertEquals(expected.removeNoise(), actual.removeNoise())
