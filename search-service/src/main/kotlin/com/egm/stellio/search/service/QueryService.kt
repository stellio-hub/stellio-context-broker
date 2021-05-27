package com.egm.stellio.search.service

import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.web.buildTemporalQuery
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.util.*
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrDefault
import org.springframework.stereotype.Service
import org.springframework.util.MultiValueMap
import java.net.URI
import java.util.*

@Service
class QueryService(
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val temporalEntityService: TemporalEntityService
) {

    fun parseAndCheckQueryParams(queryParams: MultiValueMap<String, String>, contextLink: String): Map<String, Any> {
        val withTemporalValues = hasValueInOptionsParam(
            Optional.ofNullable(queryParams.getFirst("options")), OptionsParamValue.TEMPORAL_VALUES
        )
        val ids = parseRequestParameter(queryParams.getFirst(QUERY_PARAM_ID)).map { it.toUri() }.toSet()
        val types = parseAndExpandRequestParameter(queryParams.getFirst(QUERY_PARAM_TYPE), contextLink)
        val temporalQuery = buildTemporalQuery(queryParams, contextLink)

        if (types.isEmpty() && temporalQuery.expandedAttrs.isEmpty())
            throw BadRequestDataException("Either type or attrs need to be present in request parameters")

        return mapOf(
            "ids" to ids,
            "types" to types,
            "temporalQuery" to temporalQuery,
            "withTemporalValues" to withTemporalValues
        )
    }

    suspend fun queryTemporalEntities(
        ids: Set<URI>,
        types: Set<String>,
        temporalQuery: TemporalQuery,
        withTemporalValues: Boolean,
        contextLink: String
    ): List<CompactedJsonLdEntity> {
        val temporalEntityAttributesResult = temporalEntityAttributeService.getForEntities(
            ids,
            types,
            temporalQuery.expandedAttrs
        ).awaitFirstOrDefault(emptyMap())

        val queryResult = temporalEntityAttributesResult.toList().map {
            Pair(
                it.first,
                it.second.map {
                    it to attributeInstanceService.search(temporalQuery, it, withTemporalValues).awaitFirst()
                }.toMap()
            )
        }

        return temporalEntityService.buildTemporalEntities(
            queryResult,
            temporalQuery,
            listOf(contextLink),
            withTemporalValues
        )
    }
}
