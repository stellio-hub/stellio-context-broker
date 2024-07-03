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
typealias TemporalValue = List<Any>
const val TEMPORAL_VALUE_NAME = "values"

typealias Range = Pair<ZonedDateTime, ZonedDateTime>

object TemporalApiResponseBuilder {
    @SuppressWarnings("LongParameterList")
    fun buildEntitiesTemporalResponse(
        entities: List<CompactedEntity>,
        total: Int,
        resourceUrl: String,
        query: TemporalEntitiesQuery,
        requestParams: MultiValueMap<String, String>,
        mediaType: MediaType,
        contexts: List<String>,
        lang: String? = null
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

        if (query.temporalQuery.hasLastN) { // if lastN > limit it throw an error earlier
            return successResponse
        }

        val attributesWhoReachedLimit = getAttributesWhoReachedLimit(entities, query)

        if (attributesWhoReachedLimit.isEmpty()) {
            return successResponse
        }

        val range = getTemporalPaginationRange(attributesWhoReachedLimit, query)

        val filteredEntities = entities.map { filterEntityInRange(it, range, query) }
        return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).apply {
            this.headers(
                successResponse.headers
            )
            this.header(
                HttpHeaders.CONTENT_RANGE,
                getHeaderRange(range)
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
        val successResponseHeader = prepareGetSuccessResponseHeaders(mediaType, contexts)

        if (query.temporalQuery.hasLastN) { // if lastN > limit it throw an error earlier
            return successResponseHeader.body(
                serializeObject(
                    entity.toFinalRepresentation(ngsiLdDataRepresentation)
                )
            )
        }

        val attributesWhoReachedLimit = getAttributesWhoReachedLimit(listOf(entity), query)

        if (attributesWhoReachedLimit.isEmpty()) {
            return successResponseHeader.body(
                serializeObject(
                    entity.toFinalRepresentation(ngsiLdDataRepresentation)
                )
            )
        }

        val range = getTemporalPaginationRange(attributesWhoReachedLimit, query)
        val filteredEntity = filterEntityInRange(entity, range, query)

        return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT)
            .apply {
                this.header(
                    HttpHeaders.CONTENT_RANGE,
                    getHeaderRange(range)
                )
                this.headers(
                    successResponseHeader.body("").headers
                )
            }.body(
                serializeObject(
                    filteredEntity.toFinalRepresentation(ngsiLdDataRepresentation)
                )
            )
    }

    private fun getAttributesWhoReachedLimit(entities: List<CompactedEntity>, query: TemporalEntitiesQuery): List<Any> {
        val temporalQuery = query.temporalQuery

        return entities.flatMap { entity ->
            entity.values.mapNotNull { attr ->

                when (attr) {
                    is List<*> -> if (attr.size >= temporalQuery.instanceLimit) attr else null
                    is Map<*, *> -> {
                        val attrSize = attr.toTemporalValuesOrNull(query)?.size ?: 1
                        if (attrSize >= temporalQuery.instanceLimit) attr else null
                    }
                    else -> null
                }
            }
        }
    }

    private fun getTemporalPaginationRange(
        attributesWhoReachedLimit: List<Any>,
        query: TemporalEntitiesQuery
    ): Range {
        val temporalQuery = query.temporalQuery
        val limit = temporalQuery.instanceLimit
        val timeProperty = temporalQuery.timeproperty.propertyName

        val allTimesByAttributes = attributesWhoReachedLimit.mapNotNull { attr ->
            when (attr) {
                is List<*> -> {
                    val temporalAttribute = attr as CompactedTemporalAttribute
                    temporalAttribute.map { it[timeProperty] }
                }
                is Map<*, *> -> {
                    attr.toTemporalValuesOrNull(query)?.map { it[1] }
                }
                else -> null
            }
        }
        val attributesTimeRanges = allTimesByAttributes.map {
            ZonedDateTime.parse(it.getOrNull(0) as String) to
                ZonedDateTime.parse(it.getOrNull(limit - 1) as String)
        }

        val discriminatingTimeRange = attributesTimeRanges.minBy { it.second }
        val rangeStart = when (temporalQuery.timerel) {
            TemporalQuery.Timerel.AFTER -> temporalQuery.timeAt ?: discriminatingTimeRange.first
            TemporalQuery.Timerel.BETWEEN -> temporalQuery.timeAt ?: discriminatingTimeRange.first
            else -> discriminatingTimeRange.first
        }

        return rangeStart to discriminatingTimeRange.second
    }

    private fun filterEntityInRange(
        entity: CompactedEntity,
        range: Range,
        query: TemporalEntitiesQuery
    ): CompactedEntity {
        return entity.keys.mapNotNull { key ->
            when (val attribute = entity[key]) {
                is List<*> -> {
                    val temporalAttribute = attribute as CompactedTemporalAttribute
                    // faster filter possible because list is already order
                    val filteredAttribute = temporalAttribute.filter { range.contain(it, query.temporalQuery) }
                    if (filteredAttribute.isNotEmpty()) key to filteredAttribute else null
                }

                is Map<*, *> -> {
                    attribute.toTemporalValuesOrNull(query)?.let {
                        return@mapNotNull key to it.filter { range.contain(it) }
                    }
                    if (range.contain(attribute as CompactedAttribute, query.temporalQuery)) key to attribute
                    else key to emptyList<CompactedAttribute>()
                }

                else -> key to (attribute ?: return@mapNotNull null)
            }
        }.toMap()
    }

    private fun Range.contain(time: ZonedDateTime) =
        (this.first >= time && time >= this.second) || (this.first <= time && time <= this.second)

    private fun Range.contain(temporalValue: TemporalValue) =
        this.contain(ZonedDateTime.parse(temporalValue[1] as String))

    private fun Range.contain(attribute: CompactedAttribute, query: TemporalQuery) =
        attribute.getAttributeDate(query) ?.let { this.contain(it) } ?: true // if no date it should not be filtered

    private fun CompactedAttribute.getAttributeDate(query: TemporalQuery) =
        this[query.timeproperty.propertyName]?.let {
            ZonedDateTime.parse(it as String)
        }

    private fun Map<*, *>.toTemporalValuesOrNull(query: TemporalEntitiesQuery) =
        if (query.withTemporalValues && this[TEMPORAL_VALUE_NAME] is List<*>)
            this[TEMPORAL_VALUE_NAME] as List<TemporalValue>
        else null

    private fun getHeaderRange(range: Range): String {
        val size = "*"
        return "date-time ${range.first.toHttpHeaderFormat()}-${range.second.toHttpHeaderFormat()}/$size"
    }
}
