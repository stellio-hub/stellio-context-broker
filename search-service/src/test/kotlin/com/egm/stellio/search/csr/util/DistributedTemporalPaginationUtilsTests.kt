package com.egm.stellio.search.csr.util

import com.egm.stellio.search.temporal.model.TemporalPagination
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.util.TemporalPaginationUtils.getPagingOffsets
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class DistributedTemporalPaginationUtilsTests {

    @ParameterizedTest
    @CsvSource("firstN,false,10,14", "lastN,false,10,14", "firstN,true,1,11", "lastN,true,1,11")
    fun `mergeTemporalPagination should select nearest local or CSR offsets`(
        limitParameter: String,
        hasMore: Boolean,
        limit: Int,
        expectedOffset: Long
    ) {
        val descending = limitParameter == "lastN"
        val forwards = if (descending) "intervalbefore" else "intervalafter"
        val backwards = if (descending) "intervalafter" else "intervalbefore"
        val links = listOf(14, 16).map { "<https://remote/entities?offsetN=$it>;rel=\"$forwards\"" } +
            listOf(6, 8).map { "</entities?offsetN=$it>;rel=\"$backwards\"" }
        val query = TemporalQuery(instanceLimit = limit, lastN = if (descending) limit else null, offsetN = 10)

        val remotePagination = links.map { getTemporalPagination(listOf(it)) }
        val pagination = (remotePagination + getPagingOffsets(query, hasMore)).mergeTemporalPagination(query)

        val lowerOffset = if (limit == 1) 9L else 8L
        val expected = if (descending) TemporalPagination(offsetBefore = expectedOffset, offsetAfter = lowerOffset)
        else TemporalPagination(offsetBefore = lowerOffset, offsetAfter = expectedOffset)
        assertEquals(expected, pagination)
    }

    @ParameterizedTest
    @CsvSource("false,8,14", "true,14,8")
    fun `mergeTemporalPagination should ignore invalid continuation offsets`(
        descending: Boolean,
        before: Long,
        after: Long
    ) {
        val query = TemporalQuery(instanceLimit = 10, lastN = if (descending) 10 else null, offsetN = 10)
        val pagination = listOf(
            TemporalPagination(offsetBefore = 10, offsetAfter = 10),
            TemporalPagination(offsetBefore = after, offsetAfter = before),
            TemporalPagination(offsetBefore = before, offsetAfter = after)
        ).mergeTemporalPagination(query)

        assertEquals(TemporalPagination(offsetBefore = before, offsetAfter = after), pagination)
    }

    @Test
    fun `getTemporalPagination should ignore invalid CSR offsets and unrelated links`() {
        val links = listOf(
            "invalid link",
            "</entities?offsetN=-1>;rel=\"intervalafter\"",
            "</entities?offsetN=oops>;rel=\"intervalafter\"",
            "</entities>;rel=\"intervalafter\"",
            "</entities?offsetN=0>;rel=\"intervalbefore\"",
            "</entities?offsetN=2>;rel=\"next\"",
            """<https://context>;rel="http://www.w3.org/ns/json-ld#context", """ +
                "<https://remote/entities?q=a,b&offsetN=7>;rel=\"intervalAfter\""
        )

        val pagination = getTemporalPagination(links)

        assertEquals(TemporalPagination(offsetBefore = 0, offsetAfter = 7), pagination)
    }

    @Test
    fun `getTemporalPagination should parse the offset when it is mixed inside other parameters`() {
        val links = listOf(
            "</entities?attrs=speed&offset=30&offsetN=7&firstN=10&type=Vehicle>;rel=\"intervalafter\""
        )

        val pagination = getTemporalPagination(links)

        assertEquals(TemporalPagination(offsetAfter = 7), pagination)
    }

    @Test
    fun `getTemporalPagination should decode offsets`() {
        val links = listOf("</entities?offsetN=%37>;rel=\"intervalafter\"")

        val pagination = getTemporalPagination(links)

        assertEquals(TemporalPagination(offsetAfter = 7), pagination)
    }
}
