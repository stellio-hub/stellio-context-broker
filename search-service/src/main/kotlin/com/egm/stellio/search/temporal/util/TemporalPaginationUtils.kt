package com.egm.stellio.search.temporal.util

import com.egm.stellio.search.temporal.model.TemporalPagination
import com.egm.stellio.search.temporal.model.TemporalQuery
import org.springframework.http.MediaType
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import org.springframework.web.util.UriComponentsBuilder
import org.springframework.web.util.UriUtils
import java.nio.charset.StandardCharsets.UTF_8

object TemporalPaginationUtils {

    fun getPagingOffsets(query: TemporalQuery, hasMore: Boolean): TemporalPagination {
        val lowerOffset = if (query.offsetN > 0) maxOf(0, query.offsetN.toLong() - query.instanceLimit) else null
        val higherOffset = if (hasMore) query.offsetN.toLong() + query.instanceLimit else null
        return if (query.hasLastN()) TemporalPagination(offsetBefore = higherOffset, offsetAfter = lowerOffset)
        else TemporalPagination(offsetBefore = lowerOffset, offsetAfter = higherOffset)
    }

    fun getPagingLinks(
        resourceUrl: String,
        requestParams: MultiValueMap<String, String>,
        pagination: TemporalPagination,
        mediaType: MediaType
    ): List<String> = buildList {
        pagination.offsetBefore?.let {
            add(buildPagingLink(resourceUrl, requestParams, it, "intervalbefore", mediaType))
        }
        pagination.offsetAfter?.let {
            add(buildPagingLink(resourceUrl, requestParams, it, "intervalafter", mediaType))
        }
    }

    private fun buildPagingLink(
        resourceUrl: String,
        requestParams: MultiValueMap<String, String>,
        offset: Long,
        relation: String,
        mediaType: MediaType
    ): String {
        val params = LinkedMultiValueMap(requestParams)
        params.set("offsetN", offset.toString())
        val path = UriComponentsBuilder.fromPath(resourceUrl).build().encode().toUriString()
        val queryString = params.flatMap { (key, values) ->
            values.map { "${UriUtils.encode(key, UTF_8)}=${UriUtils.encode(it, UTF_8)}" }
        }.joinToString("&")
        val url = "$path?$queryString"
        return """
            <$url>;rel="$relation";type="$mediaType"
        """.trimIndent()
    }
}
