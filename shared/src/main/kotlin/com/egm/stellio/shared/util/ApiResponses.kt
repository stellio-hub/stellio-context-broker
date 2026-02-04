package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap

fun missingPathErrorResponse(errorMessage: String): ResponseEntity<*> {
    return BadRequestDataException(errorMessage).toErrorResponse()
}

fun buildQueryResponse(
    entities: Any,
    count: Int,
    resourceUrl: String,
    paginationQuery: PaginationQuery,
    requestParams: MultiValueMap<String, String>,
    mediaType: MediaType,
    contexts: List<String>
): ResponseEntity<String> =
    buildQueryResponse(
        serializeObject(entities),
        count,
        resourceUrl,
        paginationQuery,
        requestParams,
        mediaType,
        contexts
    )

fun buildQueryResponse(
    body: String,
    count: Int,
    resourceUrl: String,
    paginationQuery: PaginationQuery,
    requestParams: MultiValueMap<String, String>,
    mediaType: MediaType,
    contexts: List<String>
): ResponseEntity<String> {
    val prevAndNextLinks = PagingUtils.getPagingLinks(
        resourceUrl,
        requestParams,
        count,
        paginationQuery.offset,
        paginationQuery.limit
    )

    val responseHeaders = prepareGetSuccessResponseHeaders(mediaType, contexts).apply {
        prevAndNextLinks.first?.let { header(HttpHeaders.LINK, it) }
        prevAndNextLinks.second?.let { header(HttpHeaders.LINK, it) }
    }

    return if (paginationQuery.count) responseHeaders.header(RESULTS_COUNT_HEADER, count.toString()).body(body)
    else responseHeaders.body(body)
}

fun prepareGetSuccessResponseHeaders(
    mediaType: MediaType,
    contexts: List<String>,
): ResponseEntity.BodyBuilder =
    ResponseEntity.status(HttpStatus.OK)
        .apply {
            if (mediaType == JSON_LD_MEDIA_TYPE) {
                this.header(HttpHeaders.CONTENT_TYPE, JSON_LD_CONTENT_TYPE)
            } else {
                this.header(HttpHeaders.LINK, buildContextLinkHeader(contexts.first()))
                this.header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            }
        }
