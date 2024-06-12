package com.egm.stellio.search.web

import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.model.CompactedAttribute
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

typealias Range = Pair<ZonedDateTime, ZonedDateTime>

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

        val range = getTemporalPaginationRange(attributesWhoReachedLimit, query)

        val filteredEntities = entities.map { filterEntityInRange(it, range, query.temporalQuery) }
        return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).apply {
            this.headers(
                successResponse.headers
            )
            this.header(
                HttpHeaders.CONTENT_RANGE,
                getHeaderRange(range, query.temporalQuery)
            )
        }.body(serializeObject(filteredEntities.toFinalRepresentation(ngsiLdDataRepresentation)))
    }

    fun buildEntityTemporalResponse(
        entity: CompactedEntity,
        query: TemporalEntitiesQuery,
        mediaType: MediaType,
        requestParams: MultiValueMap<String, String>,
        contexts: List<String>,
    ): ResponseEntity<String> {
        val ngsiLdDataRepresentation = parseRepresentations(requestParams, mediaType)

        val attributesWhoReachedLimit = getAttributesWhoReachedLimit(listOf(entity), query)

        val successResponse = prepareGetSuccessResponseHeaders(mediaType, contexts).body(
            serializeObject(
                entity.toFinalRepresentation(ngsiLdDataRepresentation)
            )
        )

        if (attributesWhoReachedLimit.isEmpty()) {
            return successResponse
        }

        val range = getTemporalPaginationRange(attributesWhoReachedLimit, query)
        val filteredEntity = filterEntityInRange(entity, range, query.temporalQuery)

        return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT)
            .apply {
                this.header(
                    HttpHeaders.CONTENT_RANGE,
                    getHeaderRange(range, query.temporalQuery)
                )
                this.headers(
                    successResponse.headers
                )
            }.body(
                serializeObject(
                    filteredEntity.toFinalRepresentation(ngsiLdDataRepresentation)
                )
            )
    }

    private fun getAttributesWhoReachedLimit(entities: List<CompactedEntity>, query: TemporalEntitiesQuery):
        List<CompactedTemporalAttribute> {
        val temporalQuery = query.temporalQuery
        return entities.flatMap { attr ->
            attr.values.mapNotNull {
                if (it is List<*> && it.size >= temporalQuery.limit) it as CompactedTemporalAttribute else null
            }
        }
    }

    private fun getTemporalPaginationRange(
        attributesWhoReachedLimit: List<CompactedTemporalAttribute>,
        query: TemporalEntitiesQuery
    ): Range {
        val temporalQuery = query.temporalQuery
        val limit = temporalQuery.limit
        val isChronological = temporalQuery.isChronological
        val timeProperty = temporalQuery.timeproperty.propertyName

        val attributesTimeRanges = attributesWhoReachedLimit.map { attribute -> attribute.map { it[timeProperty] } }
            .map {
                ZonedDateTime.parse(it.getOrNull(0) as String) to
                    ZonedDateTime.parse(it.getOrNull(limit - 1) as String)
            }

        return if (isChronological) {
            val discriminatingTimeRange = attributesTimeRanges.minBy { it.second }
            val rangeStart = when (temporalQuery.timerel) {
                TemporalQuery.Timerel.AFTER -> temporalQuery.timeAt ?: discriminatingTimeRange.first
                TemporalQuery.Timerel.BETWEEN -> temporalQuery.timeAt ?: discriminatingTimeRange.first
                else -> discriminatingTimeRange.first
            }

            rangeStart to discriminatingTimeRange.second
        } else {
            val discriminatingTimeRange = attributesTimeRanges.maxBy { it.second }
            val rangeStart = when (temporalQuery.timerel) {
                TemporalQuery.Timerel.BEFORE -> temporalQuery.timeAt ?: discriminatingTimeRange.first
                TemporalQuery.Timerel.BETWEEN -> temporalQuery.endTimeAt ?: discriminatingTimeRange.first
                else -> discriminatingTimeRange.first
            }

            rangeStart to discriminatingTimeRange.second
        }
    }

    private fun filterEntityInRange(entity: CompactedEntity, range: Range, query: TemporalQuery): CompactedEntity {
        return entity.keys.mapNotNull { key ->
            when (val attribute = entity[key]) {
                is List<*> -> {
                    val temporalAttribute = attribute as CompactedTemporalAttribute
                    // faster filter possible because list is already order
                    val filteredAttribute = temporalAttribute.filter { range.contain(it, query) }
                    if (filteredAttribute.isNotEmpty()) key to filteredAttribute else null
                }

                is Map<*, *> -> {
                    val compactAttribute: CompactedAttribute = attribute as CompactedAttribute
                    if (range.contain(compactAttribute, query)) key to attribute
                    else key to emptyList<CompactedAttribute>()
                }

                else -> key to (attribute ?: return@mapNotNull null)
            }
        }.toMap()
    }

    private fun Range.contain(time: ZonedDateTime) =
        (this.first > time && time > this.second) || (this.first < time && time < this.second)

    private fun Range.contain(attribute: CompactedAttribute, query: TemporalQuery) = this.contain(
        attribute.getAttributeDate(query)
    )

    private fun CompactedAttribute.getAttributeDate(query: TemporalQuery) = ZonedDateTime.parse(
        this[query.timeproperty.propertyName] as String
    )

    private fun getHeaderRange(range: Range, temporalQuery: TemporalQuery): String {
        val size = if (temporalQuery.isChronological) "*" else temporalQuery.limit
        return "DateTime ${range.first.toHttpHeaderFormat()}-${range.second.toHttpHeaderFormat()}/$size"
    }
}
