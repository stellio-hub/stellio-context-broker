package com.egm.stellio.search.web

import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.toFinalRepresentation
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.buildQueryResponse
import com.egm.stellio.shared.util.parseRepresentations
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
    ): ResponseEntity<String> {
        val ngsiLdDataRepresentation = parseRepresentations(requestParams, mediaType)

        lang?.let { ngsiLdDataRepresentation.copy(languageFilter = it) }

        val successResponse = buildQueryResponse(
            entities.toFinalRepresentation(ngsiLdDataRepresentation),
            total,
            resourceUrl,
            query.entitiesQuery.paginationQuery,
            requestParams,
            mediaType,
            contexts
        )

        if (range == null) {
            return successResponse
        }

        return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).apply {
            this.headers(
                successResponse.headers
            )
            this.header(
                HttpHeaders.CONTENT_RANGE,
                getHeaderRange(range, query.temporalQuery)
            )
        }.body(serializeObject(entities.toFinalRepresentation(ngsiLdDataRepresentation)))
    }

    fun buildEntityTemporalResponse(
        mediaType: MediaType,
        contexts: List<String>,
        query: TemporalEntitiesQuery,
        range: Range?
    ): ResponseEntity.BodyBuilder {
        val successResponseHeader = prepareGetSuccessResponseHeaders(mediaType, contexts)
        if (range == null) return successResponseHeader
        return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT)
            .apply {
                this.header(
                    HttpHeaders.CONTENT_RANGE,
                    getHeaderRange(range, query.temporalQuery)
                )
                this.headers(
                    successResponseHeader.body("").headers
                )
            }
    }

    private fun getHeaderRange(range: Range, temporalQuery: TemporalQuery): String {
        val size = temporalQuery.lastN ?: "*"
        return "date-time ${range.first.toHttpHeaderFormat()}-${range.second.toHttpHeaderFormat()}/$size"
    }
}
