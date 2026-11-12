package com.egm.stellio.search.temporal.web

import arrow.core.raise.either
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalPagination
import com.egm.stellio.search.temporal.util.TemporalPaginationUtils.getPagingLinks
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.NgsiLdDataRepresentation.Companion.parseRepresentations
import com.egm.stellio.shared.model.toFinalRepresentation
import com.egm.stellio.shared.util.buildQueryResponse
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap

object TemporalApiResponses {
    @SuppressWarnings("LongParameterList")
    fun buildEntitiesTemporalResponse(
        entities: List<CompactedEntity>,
        total: Int,
        resourceUrl: String,
        query: TemporalEntitiesQuery,
        requestParams: MultiValueMap<String, String>,
        mediaType: MediaType,
        contexts: List<String>,
        pagination: TemporalPagination,
        lang: String? = null,
    ): ResponseEntity<*> = either {
        val baseRepresentation = parseRepresentations(requestParams, mediaType).bind()
        // this is needed for queryEntitiesViaPost where the properties are not in the query parameters
        val representation = lang?.let {
            baseRepresentation.copy(languageFilter = it, timeproperty = query.temporalQuery.timeproperty.propertyName)
        }
            ?: baseRepresentation.copy(timeproperty = query.temporalQuery.timeproperty.propertyName)

        val successResponse = buildQueryResponse(
            entities.toFinalRepresentation(representation),
            total,
            resourceUrl,
            query.entitiesQuery.paginationQuery,
            requestParams,
            mediaType,
            contexts
        )

        val links = getPagingLinks(resourceUrl, requestParams, pagination, mediaType)
        ResponseEntity.ok().headers(successResponse.headers).apply {
            links.forEach { header(HttpHeaders.LINK, it) }
        }.body(successResponse.body)
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    fun buildEntityTemporalResponse(
        mediaType: MediaType,
        contexts: List<String>,
        pagination: TemporalPagination,
        resourceUrl: String,
        requestParams: MultiValueMap<String, String>
    ): ResponseEntity.BodyBuilder =
        prepareGetSuccessResponseHeaders(mediaType, contexts).apply {
            getPagingLinks(resourceUrl, requestParams, pagination, mediaType).forEach {
                header(HttpHeaders.LINK, it)
            }
        }
}
