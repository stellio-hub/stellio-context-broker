package com.egm.stellio.shared.util

import org.springframework.util.MultiValueMap

object PagingUtils {

    fun getPagingLinks(
        resourceUrl: String,
        requestParams: MultiValueMap<String, String>,
        resourcesCount: Int,
        offset: Int,
        limit: Int
    ): Pair<String?, String?> {
        var prevLink: String? = null
        var nextLink: String? = null

        if (offset > 0 && resourcesCount > offset - limit) {
            val prevOffset = if (offset > limit) offset - limit else 0
            prevLink =
                "<$resourceUrl?${requestParams.toEncodedUrl(offset = prevOffset, limit = limit)}>;" +
                "rel=\"prev\";type=\"application/ld+json\""
        }

        if (resourcesCount > offset + limit)
            nextLink =
                "<$resourceUrl?${requestParams.toEncodedUrl(offset = offset + limit, limit = limit)}>;" +
                "rel=\"next\";type=\"application/ld+json\""

        return Pair(prevLink, nextLink)
    }

    private fun MultiValueMap<String, String>.toEncodedUrl(offset: Int, limit: Int): String {
        val requestParams = this.entries.filter { !listOf("offset", "limit").contains(it.key) }.toMutableList()
        requestParams.addAll(mutableMapOf("limit" to listOf(limit.toString())).entries)
        requestParams.addAll(mutableMapOf("offset" to listOf(offset.toString())).entries)
        return requestParams.joinToString("&") { it.key + "=" + it.value.joinToString(",") }
    }
}
