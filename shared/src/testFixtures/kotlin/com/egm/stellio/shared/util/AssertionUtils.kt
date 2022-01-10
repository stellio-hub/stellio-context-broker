package com.egm.stellio.shared.util

import org.junit.jupiter.api.Assertions.assertEquals

fun assertJsonPayloadsAreEqual(expectation: String, actual: String) =
    assertEquals(mapper.readTree(expectation), mapper.readTree(actual))
