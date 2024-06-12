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

typealias CompactedTemporalAttribute = List<Map<String, Any>>

object TemporalApiResponse {
    @SuppressWarnings("LongParameterList")
    fun buildListTemporalResponse(
        entities: List<CompactedEntity>,
        total: Int,
        resourceUrl: String,
        query: TemporalEntitiesQuery,
        requestParams: MultiValueMap<String, String>,
        mediaType: MediaType,
        contexts: List<String>
    ): ResponseEntity<String> {
        val ngsiLdDataRepresentation = parseRepresentations(requestParams, mediaType)

        val successResponse = buildQueryResponse(
            entities.toFinalRepresentation(ngsiLdDataRepresentation),
            total,
            resourceUrl,
            query.entitiesQuery.paginationQuery,
            requestParams,
            mediaType,
            contexts
        )
        val attributesWhoReachedLimit = getAttributesWhoReachedLimit(entities, query)

        if (attributesWhoReachedLimit.isEmpty()) {
            return successResponse
        }

        return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).apply {
            this.headers(
                successResponse.headers
            )
            this.header(
                HttpHeaders.CONTENT_RANGE,
                getTemporalPaginationRange(attributesWhoReachedLimit, query)
            )
        }.build()
    }

    fun buildEntityTemporalResponse(
        entity: CompactedEntity,
        query: TemporalEntitiesQuery,
        mediaType: MediaType,
        requestParams: MultiValueMap<String, String>,
        contexts: List<String>,
    ): ResponseEntity<String> {
        val ngsiLdDataRepresentation = parseRepresentations(requestParams, mediaType)

        val successResponse = prepareGetSuccessResponseHeaders(mediaType, contexts).body(
            serializeObject(
                entity.toFinalRepresentation(ngsiLdDataRepresentation)
            )
        )

        val attributesWhoReachedLimit = getAttributesWhoReachedLimit(listOf(entity), query)

        if (attributesWhoReachedLimit.isEmpty()) {
            return successResponse
        }

        return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT)
            .apply {
                this.header(
                    HttpHeaders.CONTENT_RANGE,
                    getTemporalPaginationRange(attributesWhoReachedLimit, query)
                )
                this.headers(
                    successResponse.headers
                )
            }.build()
    }

    private fun getAttributesWhoReachedLimit(entities: List<CompactedEntity>, query: TemporalEntitiesQuery):
        List<CompactedTemporalAttribute> {
        val temporalQuery = query.temporalQuery
        return entities.flatMap {
            it.values.mapNotNull {
                if (it is List<*> && it.size >= temporalQuery.limit) it as CompactedTemporalAttribute else null
            }
        }
    }

    private fun getTemporalPaginationRange(
        attributesWhoReachedLimit: List<CompactedTemporalAttribute>,
        query: TemporalEntitiesQuery
    ): String {
        val temporalQuery = query.temporalQuery
        val limit = temporalQuery.limit
        val isChronological = temporalQuery.isChronological
        val timeProperty = temporalQuery.timeproperty.propertyName

        val attributesTimeRanges = attributesWhoReachedLimit.map { attribute -> attribute.map { it[timeProperty] } }
            .map {
                ZonedDateTime.parse(it.getOrNull(0) as String) to
                    ZonedDateTime.parse(it.getOrNull(limit - 1) as String)
            }

        val range = if (isChronological) {
            val discriminatingTimeRange = attributesTimeRanges.minBy { it.second }
            val rangeStart = when (temporalQuery.timerel) {
                TemporalQuery.Timerel.AFTER -> temporalQuery.timeAt
                TemporalQuery.Timerel.BETWEEN -> temporalQuery.timeAt
                else -> discriminatingTimeRange.first
            }

            rangeStart to discriminatingTimeRange.second
        } else {
            val discriminatingTimeRange = attributesTimeRanges.maxBy { it.second }
            val rangeStart = when (temporalQuery.timerel) {
                TemporalQuery.Timerel.BEFORE -> temporalQuery.timeAt
                TemporalQuery.Timerel.BETWEEN -> temporalQuery.endTimeAt
                else -> discriminatingTimeRange.first
            }

            rangeStart to discriminatingTimeRange.second
        }

        val size = limit.toString()
        return "DateTime ${range.first?.toHttpHeaderFormat()}-${range.second.toHttpHeaderFormat()}/$size"
    }
}
