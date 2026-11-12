package com.egm.stellio.search.temporal.util

import com.egm.stellio.search.temporal.model.TemporalPagination
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.util.TemporalPaginationUtils.getPagingOffsets
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.http.MediaType
import org.springframework.util.LinkedMultiValueMap

class TemporalPaginationUtilsTests {

    @Test
    fun `getPagingLinks should preserve query parameters and media type in both temporal directions`() {
        for (descending in listOf(false, true)) {
            val params = LinkedMultiValueMap<String, String>()
            val limitParameter = if (descending) "lastN" else "firstN"
            params.add(limitParameter, "2")
            params.add("q", "name==\"a & b\"")
            params.add("offset", "30")
            params.add("limit", "10")
            params.add("timerel", "after")
            params.add("timeAt", "2026-01-01T00:00:00+01:00")
            val links = TemporalPaginationUtils.getPagingLinks(
                "/ngsi-ld/v1/temporal/entities",
                params,
                TemporalPagination(offsetBefore = 1, offsetAfter = 5),
                MediaType.APPLICATION_JSON
            )
            assertEquals(2, links.size)
            assertTrue(links[0].contains("offsetN=1"))
            assertTrue(links[1].contains("offsetN=5"))
            assertTrue(links[0].contains("rel=\"intervalbefore\""))
            assertTrue(links[1].contains("rel=\"intervalafter\""))
            links.forEach {
                assertTrue(it.contains("type=\"application/json\""))
                assertTrue(it.contains("offset=30"))
                assertTrue(it.contains("limit=10"))
                assertTrue(it.contains("$limitParameter=2"))
                assertTrue(it.contains("a%20%26%20b"))
                assertTrue(it.contains("timeAt=2026-01-01T00%3A00%3A00%2B01%3A00"))
            }
            assertEquals(null, params.getFirst("offsetN"))
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `getPagingLinks should omit links at temporal pagination boundaries`(descending: Boolean) {
        val query = TemporalQuery(instanceLimit = 5, lastN = if (descending) 5 else null)
        val forwards = if (descending) "intervalbefore" else "intervalafter"
        val backwards = if (descending) "intervalafter" else "intervalbefore"
        val params = LinkedMultiValueMap<String, String>()
        val emptyLinks = TemporalPaginationUtils.getPagingLinks(
            "/entities",
            params,
            getPagingOffsets(query, false),
            JSON_LD_MEDIA_TYPE
        )
        assertTrue(emptyLinks.isEmpty())
        val links = TemporalPaginationUtils.getPagingLinks(
            "/entities",
            params,
            getPagingOffsets(query, true),
            JSON_LD_MEDIA_TYPE
        )
        assertEquals(1, links.size)
        assertFalse(links.single().contains("firstN="))
        assertFalse(links.single().contains("lastN="))
        assertTrue(links.single().contains(forwards))
        val reverse = TemporalPaginationUtils.getPagingLinks(
            "/entities",
            params,
            getPagingOffsets(query.copy(offsetN = 2), false),
            JSON_LD_MEDIA_TYPE
        ).single()
        assertTrue(reverse.contains("offsetN=0"))
        assertTrue(reverse.contains(backwards))
        assertFalse(reverse.contains(forwards))
    }
}
