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

    private fun MultiValueMap<String, String>.toEncodedUrl(offset: Int, limit: Int): String {
        val requestParams = this.entries.filter { !listOf("offset", "limit").contains(it.key) }.toMutableList()
        requestParams.addAll(mutableMapOf("limit" to listOf(limit.toString())).entries)
        requestParams.addAll(mutableMapOf("offset" to listOf(offset.toString())).entries)
        return requestParams.joinToString("&") { it.key + "=" + it.value.joinToString(",") }
    }
}
