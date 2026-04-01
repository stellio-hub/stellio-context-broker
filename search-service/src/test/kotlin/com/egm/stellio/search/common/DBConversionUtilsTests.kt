package com.egm.stellio.search.common

import com.egm.stellio.search.common.util.toJson
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
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
}
