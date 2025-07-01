package com.egm.stellio.subscription.model

import com.egm.stellio.subscription.model.Endpoint.Companion.parseEndpointInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class EndpointInfoTests {

    @Test
    fun `it should correctly parse an endpoint info`() {
        val input = """[{"key": "Authorization-token", "value": "Authorization-token-value"}]"""
        val info = parseEndpointInfo(input)

        assertEquals(info, listOf(EndpointInfo(key = "Authorization-token", value = "Authorization-token-value")))
    }

    @Test
    fun `it should correctly parse a null endpoint info`() {
        val input = null
        val info = parseEndpointInfo(input)

        assertEquals(info, null)
    }
}
