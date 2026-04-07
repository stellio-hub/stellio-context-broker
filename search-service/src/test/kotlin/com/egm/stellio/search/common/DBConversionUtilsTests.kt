package com.egm.stellio.search.common

import com.egm.stellio.search.common.util.toJson
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import kotlin.test.assertEquals

@ActiveProfiles("test")
class DBConversionUtilsTests {

    @Test
    fun `toJson should work for a simple string`() = runTest {
        val string = "test"
        val jsonString = string.toJson()
        assertEquals(jsonString.asString(), "\"test\"")
    }

    @Test
    fun `toJson should work for a boolean`() = runTest {
        val boolean = true
        val jsonBoolean = boolean.toJson()
        assertEquals(jsonBoolean.asString(), "true")
    }

    @Test
    fun `toJson should work for a double`() = runTest {
        val jsonBoolean = 12.0.toJson()
        assertEquals(jsonBoolean.asString(), "12.0")
    }

    @Test
    fun `toJson should work for a list`() = runTest {
        val jsonList = listOf(12.0, 13).toJson()
        assertEquals(jsonList.asString(), "[12.0,13]")
    }

    @Test
    fun `toJson should work for a map`() = runTest {
        val jsonList = mapOf("property1" to 1, "property2" to 2).toJson()
        assertEquals(jsonList.asString(), """{"property1":1,"property2":2}""")
    }

    @Test
    fun `toJson should work for a URI`() = runTest {
        val jsonList = URI("urn:ngsi-ld:1").toJson()
        assertEquals("\"urn:ngsi-ld:1\"", jsonList.asString())
    }
}
