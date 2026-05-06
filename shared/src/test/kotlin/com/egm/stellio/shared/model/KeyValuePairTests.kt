package com.egm.stellio.shared.model

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class KeyValuePairTests {

    @Test
    fun `it should correctly deserialize a key value pair`() {
        val input = """[{"key": "Authorization-token", "value": "Authorization-token-value"}]"""
        val info = KeyValuePair.deserialize(input)

        Assertions.assertEquals(
            listOf(KeyValuePair(key = "Authorization-token", value = "Authorization-token-value")),
            info
        )
    }

    @Test
    fun `it should correctly deserialize a null a key value pair`() {
        val input = null
        val info = KeyValuePair.deserialize(input)

        Assertions.assertEquals(null, info)
    }
}
