package com.egm.stellio.search.entity.service

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.getRelationshipsObjects
import com.egm.stellio.shared.model.inlineLinkedEntities
import com.egm.stellio.shared.queryparameter.LinkedEntityQuery
import com.egm.stellio.shared.queryparameter.LinkedEntityQuery.Companion.JoinType
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.Sub
import org.springframework.stereotype.Service

@Service
class LinkedEntityService(
    private val entityQueryService: EntityQueryService
) {
    suspend fun processLinkedEntities(
        compactedEntity: CompactedEntity,
        entitiesQuery: EntitiesQuery,
        sub: Sub?
    ): Either<APIException, List<CompactedEntity>> = either {
        val linkedEntityQuery = entitiesQuery.linkedEntityQuery
        if (linkedEntityQuery == null || linkedEntityQuery.join == JoinType.NONE)
            return listOf(compactedEntity).right()

        enrichWithLinkedEntities(
            listOf(compactedEntity),
            linkedEntityQuery,
            entitiesQuery.contexts,
            1.toUInt(),
            sub
        ).bind()
    }

    suspend fun processLinkedEntities(
        compactedEntities: List<CompactedEntity>,
        entitiesQuery: EntitiesQuery,
        sub: Sub?
    ): Either<APIException, List<CompactedEntity>> = either {
        val linkedEntityQuery = entitiesQuery.linkedEntityQuery
        if (linkedEntityQuery == null || linkedEntityQuery.join == JoinType.NONE)
            return compactedEntities.right()

        enrichWithLinkedEntities(compactedEntities, linkedEntityQuery, entitiesQuery.contexts, 1.toUInt(), sub).bind()
    }

    internal suspend fun enrichWithLinkedEntities(
        compactedEntities: List<CompactedEntity>,
        linkedEntityQuery: LinkedEntityQuery,
        contexts: List<String>,
        currentLevel: UInt,
        sub: Sub?
    ): Either<APIException, List<CompactedEntity>> = either {
        val linkedUris = compactedEntities.getRelationshipsObjects()
        if (currentLevel > linkedEntityQuery.joinLevel || linkedUris.isEmpty())
            return compactedEntities.right()

        val relationshipsQuery = EntitiesQueryFromGet(
            ids = linkedUris,
            paginationQuery = PaginationQuery(0, Int.MAX_VALUE),
            contexts = contexts
        )
        val linkedEntities = entityQueryService.queryEntities(relationshipsQuery, sub).bind()
            .first
            .let { compactEntities(it, contexts) }

        when (linkedEntityQuery.join) {
            JoinType.FLAT ->
                flattenLinkedEntities(
                    compactedEntities,
                    enrichWithLinkedEntities(
                        linkedEntities,
                        linkedEntityQuery,
                        contexts,
                        currentLevel.inc(),
                        sub
                    ).bind()
                )
            JoinType.INLINE ->
                inlineLinkedEntities(
                    compactedEntities,
                    enrichWithLinkedEntities(
                        linkedEntities,
                        linkedEntityQuery,
                        contexts,
                        currentLevel.inc(),
                        sub
                    ).bind()
                )
            // not possible but it needs to be handled anyway
            else -> compactedEntities
        }
    }

    internal fun inlineLinkedEntities(
        compactedEntities: List<CompactedEntity>,
        linkedEntities: List<CompactedEntity>
    ): List<CompactedEntity> =
        compactedEntities.inlineLinkedEntities(
            // remove the inner contexts when inline mode, the context is the one from the linking entity
            linkedEntities.map { it.minus(JSONLD_CONTEXT_KW) }
                .associateBy { it[NGSILD_ID_TERM] as String }
        )

    internal fun flattenLinkedEntities(
        compactedEntities: List<CompactedEntity>,
        linkedEntities: List<CompactedEntity>
    ): List<CompactedEntity> =
        compactedEntities + linkedEntities
}
