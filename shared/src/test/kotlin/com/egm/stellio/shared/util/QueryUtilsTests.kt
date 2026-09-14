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

    @Test
    fun `buildScopeQQuery should escape single quotes in an exact match scope`() = runTest {
        assertEquals(
            """
            exists (select * from unnest(scopes) as scope
            where scope = '/agri''food')
            """.trimIndent(),
            buildScopeQQuery("/agri'food")
        )
    }

    @Test
    fun `buildScopeQQuery should escape single quotes in a pattern-based scope`() = runTest {
        assertEquals(
            """
            exists (select * from unnest(scopes) as scope
            where scope similar to '/agri''food%')
            """.trimIndent(),
            buildScopeQQuery("/agri'food#")
        )
    }
}
