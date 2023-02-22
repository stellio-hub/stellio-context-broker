package com.egm.stellio.shared.util

import arrow.core.Either
import com.egm.stellio.shared.model.APIException
import org.junit.jupiter.api.Assertions.*

fun assertJsonPayloadsAreEqual(expectation: String, actual: String) =
    assertEquals(mapper.readTree(expectation), mapper.readTree(actual))

fun <T> Either<APIException, T>.shouldSucceedWith(assertions: (T) -> Unit) =
    fold({
        fail("it should have not returned an error $it")
    }, {
        assertions(it)
    })

fun Either<APIException, Unit>.shouldSucceed() =
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
