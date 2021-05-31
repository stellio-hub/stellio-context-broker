package com.egm.stellio.shared.util

import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap

object PagingUtils {

    fun getPagingLinks(
        resourceUrl: String,
        requestParams: MultiValueMap<String, String>,
        resourcesCount: Int,
        pageNumber: Int,
        limit: Int
    ): Pair<String?, String?> {
        var prevLink: String? = null
        var nextLink: String? = null

        if (pageNumber > 1 && (resourcesCount > (pageNumber - 1) * limit))
            prevLink =
                "<$resourceUrl?${requestParams.toEncodedUrl(page = pageNumber - 1, limit = limit)}>;" +
                "rel=\"prev\";type=\"application/ld+json\""

        if (resourcesCount > pageNumber * limit)
            nextLink =
                "<$resourceUrl?${requestParams.toEncodedUrl(page = pageNumber + 1, limit = limit)}>;" +
                "rel=\"next\";type=\"application/ld+json\""

        return Pair(prevLink, nextLink)
    }

    fun buildPaginationResponse(
        body: String,
        resourcesCount: Int,
        count: Boolean,
        prevAndNextLinks: Pair<String?, String?>,
        mediaType: MediaType,
        contextLink: String
    ): ResponseEntity<String> {
        val responseHeaders = if (prevAndNextLinks.first != null && prevAndNextLinks.second != null)
            buildGetSuccessResponse(mediaType, contextLink)
                .header(HttpHeaders.LINK, prevAndNextLinks.first)
                .header(HttpHeaders.LINK, prevAndNextLinks.second)

        else if (prevAndNextLinks.first != null)
            buildGetSuccessResponse(mediaType, contextLink)
                .header(HttpHeaders.LINK, prevAndNextLinks.first)
        else if (prevAndNextLinks.second != null)
            buildGetSuccessResponse(mediaType, contextLink)
                .header(HttpHeaders.LINK, prevAndNextLinks.second)
        else
            buildGetSuccessResponse(mediaType, contextLink)

        return if (count) responseHeaders.header(RESULTS_COUNT_HEADER, resourcesCount.toString()).body(body)
        else responseHeaders.body(body)
    }

    private fun MultiValueMap<String, String>.toEncodedUrl(page: Int, limit: Int): String {
        val requestParams = this.entries.filter { !listOf("page", "limit").contains(it.key) }.toMutableList()
        requestParams.addAll(mutableMapOf("limit" to listOf(limit.toString())).entries)
        requestParams.addAll(mutableMapOf("page" to listOf(page.toString())).entries)
        return requestParams.joinToString("&") { it.key + "=" + it.value.joinToString(",") }
    }
}
