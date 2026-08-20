package com.egm.stellio.shared.util

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test")
class QueryUtilsTests {

    @Test
    fun `toSqlArray should escape single quotes`() = runTest {
        assertEquals("ARRAY['placed''italie']", listOf("placed'italie").toSqlArray())
    }

    @Test
    fun `toSqlList should escape single quotes`() = runTest {
        assertEquals("('placed''italie')", listOf("placed'italie").toSqlList())
    }
}
