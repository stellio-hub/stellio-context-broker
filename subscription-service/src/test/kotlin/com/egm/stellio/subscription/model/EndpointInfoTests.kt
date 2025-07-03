package com.egm.stellio.subscription.model

import com.egm.stellio.subscription.model.Endpoint.Companion.deserialize
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class EndpointInfoTests {

    @Test
    fun `it should correctly deserialize an endpoint info`() {
        val input = """[{"key": "Authorization-token", "value": "Authorization-token-value"}]"""
        val info = deserialize(input)

        assertEquals(listOf(EndpointInfo(key = "Authorization-token", value = "Authorization-token-value")), info)
    }

    @Test
    fun `it should correctly deserialize a null endpoint info`() {
        val input = null
        val info = deserialize(input)

        assertEquals(null, info)
    }
}
