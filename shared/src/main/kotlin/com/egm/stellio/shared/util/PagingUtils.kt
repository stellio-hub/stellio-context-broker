package com.egm.stellio.shared.util

object PagingUtils {

    const val SUBSCRIPTION_QUERY_PAGING_LIMIT = 30

    fun getSubscriptionsPagingLinks(subscriptionsCount: Int, pageNumber: Int, limit: Int): Pair<String?, String?> {
        var prevLink: String? = null
        var nextLink: String? = null

        if (pageNumber > 1 && (subscriptionsCount > (pageNumber - 1) * limit))
            prevLink =
                """
                </ngsi-ld/v1/subscriptions?limit=$limit&page=${pageNumber - 1}>;rel="prev";type="application/ld+json"
                """.trimIndent()
        if (subscriptionsCount > pageNumber * limit)
            nextLink =
                """
                </ngsi-ld/v1/subscriptions?limit=$limit&page=${pageNumber + 1}>;rel="next";type="application/ld+json"
                """.trimIndent()

        return Pair(prevLink, nextLink)
    }
}
