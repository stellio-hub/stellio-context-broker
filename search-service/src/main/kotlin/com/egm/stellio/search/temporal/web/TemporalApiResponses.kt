package com.egm.stellio.search.temporal.web

import arrow.core.raise.either
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.NgsiLdDataRepresentation.Companion.parseRepresentations
import com.egm.stellio.shared.model.toFinalRepresentation
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.buildQueryResponse
import com.egm.stellio.shared.util.prepareGetSuccessResponseHeaders
import com.egm.stellio.shared.util.toHttpHeaderFormat
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import java.time.ZonedDateTime

typealias Range = Pair<ZonedDateTime, ZonedDateTime>

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
        range: Range?,
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

        if (range == null)
            successResponse
        else ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).apply {
            this.headers(
                successResponse.headers
            )
            this.header(
                HttpHeaders.CONTENT_RANGE,
                getHeaderRange(range, query.temporalQuery)
            )
        }.body(serializeObject(entities.toFinalRepresentation(representation)))
    }.fold(
        { it.toErrorResponse() },
        { it }
    )

    fun buildEntityTemporalResponse(
        mediaType: MediaType,
        contexts: List<String>,
        query: TemporalEntitiesQuery,
        range: Range?
    ): ResponseEntity.BodyBuilder {
        val successResponseHeader = prepareGetSuccessResponseHeaders(mediaType, contexts)

        return if (range == null)
            successResponseHeader
        else ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).apply {
            this.headers(
                successResponseHeader.body("").headers
            )
            this.header(
                HttpHeaders.CONTENT_RANGE,
                getHeaderRange(range, query.temporalQuery)
            )
        }
    }

    private fun getHeaderRange(range: Range, temporalQuery: TemporalQuery): String {
        val size = temporalQuery.lastN ?: "*"
        return "date-time ${range.first.toHttpHeaderFormat()}-${range.second.toHttpHeaderFormat()}/$size"
    }
}
