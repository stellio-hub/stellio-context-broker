package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
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
            where scope similar to '/agri''food(/%)?')
            """.trimIndent(),
            buildScopeQQuery("/agri'food/#")
        )
    }

    @Test
    fun `buildScopeQQuery should translate + to a single level and escape _ in a pattern-based scope`() = runTest {
        assertEquals(
            """
            exists (select * from unnest(scopes) as scope
            where scope similar to '/a\_b/[^/]+(/%)?')
            """.trimIndent(),
            buildScopeQQuery("/a_b/+/#")
        )
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "/Madrid", "/Madrid/Gardens/#", "/#", "/Madrid/+/ParqueNorte", "/public/+/#", "/Città_1/Zone2",
            "(/Madrid/Districts;/CompanyA)|/CompanyB", "(/Madrid/Districts;/CompanyA),/CompanyB/#", "(/a)",
            "/a,/b", "/a|/b", "a", "levelA/levelB", "levelA/+/#", "(a;/b)|c/#"
        ]
    )
    fun `validateScopeQ should accept a valid scope query`(scopeQ: String) {
        validateScopeQ(scopeQ).shouldSucceedWith { assertEquals(scopeQ, it) }
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "/a,,/b", "(/a", "/a;", "/a)", "1a", "/1a", "a//b", "//a", "/a/", "/a/#/b", "/a#", "/a b", "/a'b",
            "((/a))", "", "/a;/b", "(/a/#)", "(/a;/b)/#", "/a|/#", "/#,/a"
        ]
    )
    fun `validateScopeQ should reject an invalid scope query`(scopeQ: String) {
        validateScopeQ(scopeQ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals("Scope query is invalid: $scopeQ", it.message)
        }
    }
}
