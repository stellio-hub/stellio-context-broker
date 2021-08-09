package com.egm.stellio.shared.util

import junit.framework.TestCase.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.util.LinkedMultiValueMap

class PagingUtilsTests {

    private val subscriptionResourceUrl = "/ngsi-ld/v1/subscriptions"

    @Test
    fun `it should not return the links`() {
        val links = PagingUtils.getPagingLinks(
            subscriptionResourceUrl,
            LinkedMultiValueMap(),
            8,
            0,
            10
        )

        assertEquals(links, Pair(null, null))
    }

    @Test
    fun `it should return the prev link`() {
        val links = PagingUtils.getPagingLinks(
            subscriptionResourceUrl,
            LinkedMultiValueMap(),
            8,
            5,
            5
        )

        assertEquals(
            links,
            Pair("</ngsi-ld/v1/subscriptions?limit=5&offset=0>;rel=\"prev\";type=\"application/ld+json\"", null)
        )
    }

    @Test
    fun `it should return the next link`() {
        val links = PagingUtils.getPagingLinks(
            subscriptionResourceUrl,
            LinkedMultiValueMap(),
            8,
            0,
            5
        )

        assertEquals(
            links,
            Pair(null, "</ngsi-ld/v1/subscriptions?limit=5&offset=5>;rel=\"next\";type=\"application/ld+json\"")
        )
    }

    @Test
    fun `it should return the prev and the next links`() {
        val links = PagingUtils.getPagingLinks(
            subscriptionResourceUrl,
            LinkedMultiValueMap(),
            8,
            2,
            3
        )

        assertEquals(
            links,
            Pair(
                "</ngsi-ld/v1/subscriptions?limit=3&offset=0>;rel=\"prev\";type=\"application/ld+json\"",
                "</ngsi-ld/v1/subscriptions?limit=3&offset=5>;rel=\"next\";type=\"application/ld+json\""
            )
        )
    }
}
