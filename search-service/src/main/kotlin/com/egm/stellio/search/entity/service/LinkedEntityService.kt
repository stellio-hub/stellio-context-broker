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
import com.egm.stellio.shared.model.filterPickAndOmit
import com.egm.stellio.shared.model.getAttributesFor
import com.egm.stellio.shared.model.getRelationshipsNamesWithObjects
import com.egm.stellio.shared.model.inlineLinkedEntities
import com.egm.stellio.shared.queryparameter.LinkedEntityQuery.Companion.JoinType
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.toUri
import org.springframework.stereotype.Service

@Service
class LinkedEntityService(
    private val entityQueryService: EntityQueryService
) {
    suspend fun processLinkedEntities(
        compactedEntity: CompactedEntity,
        entitiesQuery: EntitiesQuery
    ): Either<APIException, List<CompactedEntity>> = either {
        processLinkedEntities(listOf(compactedEntity), entitiesQuery).bind()
    }

    suspend fun processLinkedEntities(
        compactedEntities: List<CompactedEntity>,
        entitiesQuery: EntitiesQuery
    ): Either<APIException, List<CompactedEntity>> = either {
        val linkedEntityQuery = entitiesQuery.linkedEntityQuery
        if (linkedEntityQuery == null || linkedEntityQuery.join == JoinType.NONE)
            return compactedEntities.right()

        enrichWithLinkedEntities(
            compactedEntities,
            entitiesQuery,
            1.toUInt()
        ).bind()
    }

    internal suspend fun enrichWithLinkedEntities(
        compactedEntities: List<CompactedEntity>,
        entitiesQuery: EntitiesQuery,
        currentLevel: UInt
    ): Either<APIException, List<CompactedEntity>> = either {
        val linkedRelationships = compactedEntities.getRelationshipsNamesWithObjects()
        if (currentLevel > entitiesQuery.linkedEntityQuery!!.joinLevel || linkedRelationships.isEmpty())
            return compactedEntities.right()

        val relationshipsQuery = EntitiesQueryFromGet(
            ids = linkedRelationships.values.flatten().toSet(),
            paginationQuery = PaginationQuery(0, Int.MAX_VALUE),
            contexts = entitiesQuery.contexts
        )
        val linkedEntities = entityQueryService.queryEntities(relationshipsQuery).bind()
            .first
            .let { compactEntities(it, entitiesQuery.contexts) }
            .let {
                it.map { compactedEntity ->
                    // If there are two relationships with a different name targeting the same entity,
                    // using .first() is incorrect (the 1st wins over the 2nd)
                    // Moreover, this use case is problematic if flatten representation is chosen
                    val parentAttributeName = linkedRelationships.filter { entry ->
                        entry.value.contains((compactedEntity[NGSILD_ID_TERM] as String).toUri())
                    }.keys.first()
                    compactedEntity.filterPickAndOmit(
                        entitiesQuery.pick.getAttributesFor(parentAttributeName, currentLevel),
                        entitiesQuery.omit.getAttributesFor(parentAttributeName, currentLevel)
                    ).bind()
                }
            }

        when (entitiesQuery.linkedEntityQuery!!.join) {
            JoinType.FLAT ->
                flattenLinkedEntities(
                    compactedEntities,
                    enrichWithLinkedEntities(
                        linkedEntities,
                        entitiesQuery,
                        currentLevel.inc()
                    ).bind()
                )
            JoinType.INLINE ->
                inlineLinkedEntities(
                    compactedEntities,
                    enrichWithLinkedEntities(
                        linkedEntities,
                        entitiesQuery,
                        currentLevel.inc()
                    ).bind()
                )
            // not possible, but it needs to be handled anyway
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
