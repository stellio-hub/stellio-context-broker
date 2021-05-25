package com.egm.stellio.search.web

import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.service.TemporalEntityService
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.util.*
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrDefault
import org.springframework.util.MultiValueMap
import reactor.core.publisher.Mono
import java.util.*

class HandlerUtils(
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val temporalEntityService: TemporalEntityService
    ) {

    fun queryTemporalEntities(
        params: MultiValueMap<String, String>,
        contextLink: String
    ): Mono<List<CompactedJsonLdEntity>> {
        val withTemporalValues =
            hasValueInOptionsParam(Optional.ofNullable(params.getFirst("options")), OptionsParamValue.TEMPORAL_VALUES)
        val ids = parseRequestParameter(params.getFirst(QUERY_PARAM_ID)).map { it.toUri() }.toSet()
        val types = parseAndExpandRequestParameter(params.getFirst(QUERY_PARAM_TYPE), contextLink)
        val temporalQuery = buildTemporalQuery(params, contextLink)
        if (types.isEmpty() && temporalQuery.expandedAttrs.isEmpty())
            throw BadRequestDataException("Either type or attrs need to be present in request parameters")

        return temporalEntityAttributeService.getForEntities(
            ids,
            types,
            temporalQuery.expandedAttrs
        ).map {
            it.toList().map {
                Pair(
                    it.first,
                    it.second.map { te ->
                        val test = attributeInstanceService.search(temporalQuery, te, withTemporalValues).map {
                            te to it
                        }
                    }.toMap()
                )
            }
        }.map {
            temporalEntityService.buildTemporalEntities(
                it,
                temporalQuery,
                listOf(contextLink),
                withTemporalValues
            )
        }

    }
}
