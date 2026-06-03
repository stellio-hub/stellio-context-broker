package com.egm.stellio.search.common

import com.egm.stellio.search.common.util.asJsonB
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import kotlin.test.assertEquals

@ActiveProfiles("test")
class DBConversionUtilsTests {

    @Test
    fun `asJsonB should work for a simple string`() = runTest {
        val string = "test"
        val jsonString = string.asJsonB()
        assertEquals(jsonString.asString(), "\"test\"")
    }

    @Test
    fun `asJsonB should work for a boolean`() = runTest {
        val boolean = true
        val jsonBoolean = boolean.asJsonB()
        assertEquals(jsonBoolean.asString(), "true")
    }

    @Test
    fun `asJsonB should work for a double`() = runTest {
        val jsonBoolean = 12.0.asJsonB()
        assertEquals(jsonBoolean.asString(), "12.0")
    }

    @Test
    fun `asJsonB should work for a list`() = runTest {
        val jsonList = listOf(12.0, 13).asJsonB()
        assertEquals(jsonList.asString(), "[12.0,13]")
    }

    @Test
    fun `asJsonB should work for a map`() = runTest {
        val jsonList = mapOf("property1" to 1, "property2" to 2).asJsonB()
        assertEquals(jsonList.asString(), """{"property1":1,"property2":2}""")
    }

    @Test
    fun `toJson should work for a URI`() = runTest {
        val jsonList = URI("urn:ngsi-ld:1").asJsonB()
        assertEquals("\"urn:ngsi-ld:1\"", jsonList.asString())
    }
}
