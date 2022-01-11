package com.egm.stellio.subscription.utils

import com.egm.stellio.subscription.model.EndpointInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ParsingUtils::class])
@ActiveProfiles("test")
class ParsingUtilsTests {

    @Test
    fun `it should correctly parse an endpoint info`() {
        val input = "[{\"key\": \"Authorization-token\", \"value\": \"Authorization-token-value\"}]"
        val info = ParsingUtils.parseEndpointInfo(input)

        assertEquals(info, listOf(EndpointInfo(key = "Authorization-token", value = "Authorization-token-value")))
    }

    @Test
    fun `it should correctly parse a null endpoint info`() {
        val input = null
        val info = ParsingUtils.parseEndpointInfo(input)

        assertEquals(info, null)
    }
}
