package com.egm.stellio.search.util

import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.TemporalEntityAttributeInstancesResult
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.service.TemporalEntityService
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.util.*
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URI
import java.util.*

@Service
class QueryUtils(
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val temporalEntityService: TemporalEntityService
) {

    fun queryTemporalEntities(
        ids: Set<URI>,
        types: Set<String>,
        temporalQuery: TemporalQuery,
        withTemporalValues: Boolean,
        contexts: List<String>
    ): Mono<List<CompactedJsonLdEntity>> {
        return temporalEntityAttributeService.getForEntities(
            ids,
            types,
            temporalQuery.expandedAttrs
        ).flatMap {
            Flux.fromIterable(it.entries).flatMap { temporalEntity ->
                Flux.fromIterable(temporalEntity.value).flatMap { temporalEntityAttribute ->
                    attributeInstanceService.search(temporalQuery, temporalEntityAttribute, withTemporalValues)
                        .map { temporalEntityAttribute to it }
                }
                    .collectList()
                    .map { Pair(temporalEntity.key, it.toMap() as TemporalEntityAttributeInstancesResult) }
            }.collectList()
        }.map {
            temporalEntityService.buildTemporalEntities(
                it,
                temporalQuery,
                contexts,
                withTemporalValues
            )
        }
    }
}
