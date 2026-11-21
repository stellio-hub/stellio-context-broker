package com.egm.stellio.search.csr.util

import com.apicatalog.jsonld.http.link.Link
import com.egm.stellio.search.temporal.model.TemporalPagination
import com.egm.stellio.search.temporal.model.TemporalQuery
import org.springframework.web.util.UriComponentsBuilder
import org.springframework.web.util.UriUtils
import java.nio.charset.StandardCharsets.UTF_8

fun getTemporalPagination(links: List<String>): TemporalPagination {
    val remote = parseRemoteOffsets(links)
    return TemporalPagination(
        offsetBefore = remote.firstOrNull { it.first == "intervalbefore" }?.second,
        offsetAfter = remote.firstOrNull { it.first == "intervalafter" }?.second
    )
}

fun Iterable<TemporalPagination>.mergeTemporalPagination(query: TemporalQuery): TemporalPagination {
    fun nearest(offsets: List<Long>, increasing: Boolean): Long? = offsets
        .filter { if (increasing) it > query.offsetN else it < query.offsetN }
        .let { if (increasing) it.minOrNull() else it.maxOrNull() }

    return TemporalPagination(
        offsetBefore = nearest(mapNotNull { it.offsetBefore }, query.hasLastN()),
        offsetAfter = nearest(mapNotNull { it.offsetAfter }, !query.hasLastN())
    )
}

private fun parseRemoteOffsets(headers: List<String>): List<Pair<String, Long>> = headers.flatMap { header ->
    runCatching { Link.of(header) }.getOrDefault(emptyList()).flatMap { link ->
        runCatching {
            val offset = UriComponentsBuilder.fromUri(link.target()).build().queryParams.getFirst("offsetN")
                ?.let { UriUtils.decode(it, UTF_8) }
                ?.toLongOrNull()?.takeIf { it >= 0 }
            if (offset == null) emptyList()
            else link.relations().map { it.lowercase() to offset }
        }.getOrDefault(emptyList())
    }
}
