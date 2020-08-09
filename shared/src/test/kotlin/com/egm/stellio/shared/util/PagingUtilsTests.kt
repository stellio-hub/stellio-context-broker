package com.egm.stellio.shared.util

import junit.framework.TestCase.assertEquals
import org.junit.jupiter.api.Test

class PagingUtilsTests {

    @Test
    fun `it should not return the links`() {

        val links = PagingUtils.getSubscriptionsPagingLinks(8, 1, 10)

        assertEquals(links, Pair(null, null))
    }

    @Test
    fun `it should return the prev link`() {

        val links = PagingUtils.getSubscriptionsPagingLinks(8, 2, 5)

        assertEquals(
            links,
            Pair("</ngsi-ld/v1/subscriptions?limit=5&page=1>;rel=\"prev\";type=\"application/ld+json\"", null)
        )
    }

    @Test
    fun `it should return the next link`() {

        val links = PagingUtils.getSubscriptionsPagingLinks(8, 1, 5)

        assertEquals(
            links,
            Pair(null, "</ngsi-ld/v1/subscriptions?limit=5&page=2>;rel=\"next\";type=\"application/ld+json\"")
        )
    }

    @Test
    fun `it should return the prev and the next links`() {

        val links = PagingUtils.getSubscriptionsPagingLinks(8, 3, 1)

        assertEquals(
            links, Pair(
                "</ngsi-ld/v1/subscriptions?limit=1&page=2>;rel=\"prev\";type=\"application/ld+json\"",
                "</ngsi-ld/v1/subscriptions?limit=1&page=4>;rel=\"next\";type=\"application/ld+json\""
            )
        )
    }
}
