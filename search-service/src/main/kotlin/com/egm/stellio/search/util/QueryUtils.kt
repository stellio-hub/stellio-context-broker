package com.egm.stellio.search.util

import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.service.TemporalEntityService
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrDefault
import org.springframework.stereotype.Service
import java.net.URI

@Service
class QueryUtils(
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val temporalEntityService: TemporalEntityService
) {

    suspend fun queryTemporalEntities(
        ids: Set<URI>,
        types: Set<String>,
        temporalQuery: TemporalQuery,
        withTemporalValues: Boolean,
        contexts: List<String>
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
            contexts,
            withTemporalValues
        )
    }
}
