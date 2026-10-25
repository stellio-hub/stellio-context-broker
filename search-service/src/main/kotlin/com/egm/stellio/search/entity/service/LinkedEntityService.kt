package com.egm.stellio.search.entity.service

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.csr.service.DistributedEntityConsumptionService
import com.egm.stellio.search.csr.util.ContextSourceUtils
import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.filterPickAndOmit
import com.egm.stellio.shared.model.getAttributesToOmitFor
import com.egm.stellio.shared.model.getAttributesToPickFor
import com.egm.stellio.shared.model.getRelationshipsNamesWithObjects
import com.egm.stellio.shared.model.getRelationshipsObjectsWithObjectTypes
import com.egm.stellio.shared.model.inlineLinkedEntities
import com.egm.stellio.shared.queryparameter.LinkedEntityQuery.Companion.JoinType
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.toTypeSelection
import com.egm.stellio.shared.util.toUri
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Service
import org.springframework.util.LinkedMultiValueMap
import java.net.URI

typealias LinkedEntitiesResult = Pair<List<CompactedEntity>, List<NGSILDWarning>>

@Service
class LinkedEntityService(
    private val entityQueryService: EntityQueryService,
    private val distributedEntityConsumptionService: DistributedEntityConsumptionService
) {
    suspend fun processLinkedEntities(
        compactedEntity: CompactedEntity,
        entitiesQuery: EntitiesQuery,
        httpHeaders: HttpHeaders
    ): Either<APIException, LinkedEntitiesResult> = either {
        processLinkedEntities(listOf(compactedEntity), entitiesQuery, httpHeaders).bind()
    }

    /**
     * Implements TS 104 175 §7.7 - Linked Entity retrieval
     *
     * Linked Entities not stored locally are retrieved from the matching Context Sources only if the Relationship
     * is annotated with an objectType and the query is not local.
     */
    suspend fun processLinkedEntities(
        compactedEntities: List<CompactedEntity>,
        entitiesQuery: EntitiesQuery,
        httpHeaders: HttpHeaders
    ): Either<APIException, LinkedEntitiesResult> = either {
        val linkedEntityQuery = entitiesQuery.linkedEntityQuery
        if (linkedEntityQuery == null || linkedEntityQuery.join == JoinType.NONE)
            return (compactedEntities to emptyList<NGSILDWarning>()).right()

        enrichWithLinkedEntities(
            compactedEntities,
            entitiesQuery,
            httpHeaders,
            1.toUInt()
        ).bind()
    }

    internal suspend fun enrichWithLinkedEntities(
        compactedEntities: List<CompactedEntity>,
        entitiesQuery: EntitiesQuery,
        httpHeaders: HttpHeaders,
        currentLevel: UInt
    ): Either<APIException, LinkedEntitiesResult> = either {
        if (currentLevel > entitiesQuery.linkedEntityQuery!!.joinLevel)
            return (compactedEntities to emptyList<NGSILDWarning>()).right()

        val linkedRelationships = compactedEntities.getRelationshipsNamesWithObjects()
        if (linkedRelationships.isEmpty())
            return (compactedEntities to emptyList<NGSILDWarning>()).right()

        val localLinkedEntities = queryLocalLinkedEntities(linkedRelationships.values.flatten().toSet(), entitiesQuery)
            .bind()
        val (warnings, mergedLinkedEntities) = mergeWithRemoteLinkedEntities(
            localLinkedEntities,
            compactedEntities.getRelationshipsObjectsWithObjectTypes(),
            entitiesQuery,
            httpHeaders
        )

        val linkedEntities = mergedLinkedEntities.map { compactedEntity ->
            // If there are two relationships with a different name targeting the same entity,
            // using .first() is incorrect (the 1st wins over the 2nd)
            // Moreover, this use case is problematic if flatten representation is chosen
            val parentAttributeName = linkedRelationships.filter { entry ->
                entry.value.contains((compactedEntity[NGSILD_ID_TERM] as String).toUri())
            }.keys.first()
            compactedEntity.filterPickAndOmit(
                entitiesQuery.pick.getAttributesToPickFor(parentAttributeName, currentLevel),
                entitiesQuery.omit.getAttributesToOmitFor(parentAttributeName, currentLevel)
            ).bind()
        }

        val (enrichedLinkedEntities, nextLevelsWarnings) =
            enrichWithLinkedEntities(linkedEntities, entitiesQuery, httpHeaders, currentLevel.inc()).bind()

        val entities = when (entitiesQuery.linkedEntityQuery!!.join) {
            JoinType.FLAT -> flattenLinkedEntities(compactedEntities, enrichedLinkedEntities)
            JoinType.INLINE -> inlineLinkedEntities(compactedEntities, enrichedLinkedEntities)
            // not possible, but it needs to be handled anyway
            else -> compactedEntities
        }
        entities to warnings + nextLevelsWarnings
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

    private suspend fun queryLocalLinkedEntities(
        ids: Set<URI>,
        entitiesQuery: EntitiesQuery
    ): Either<APIException, List<CompactedEntity>> = either {
        val relationshipsQuery = EntitiesQueryFromGet(
            ids = ids,
            paginationQuery = PaginationQuery(0, Int.MAX_VALUE),
            contexts = entitiesQuery.contexts
        )
        entityQueryService.queryEntities(relationshipsQuery).bind()
            .first
            .let { compactEntities(it, entitiesQuery.contexts) }
    }

    // the Linked Entities annotated with an objectType are also queried from the matching CSRs, even when stored
    // locally, to be merged with the local ones as for the main Retrieve / Query Entities operations
    private suspend fun mergeWithRemoteLinkedEntities(
        localLinkedEntities: List<CompactedEntity>,
        objectsWithTypes: Map<URI, Set<String>>,
        entitiesQuery: EntitiesQuery,
        httpHeaders: HttpHeaders
    ): Pair<List<NGSILDWarning>, List<CompactedEntity>> {
        if (objectsWithTypes.isEmpty() || entitiesQuery.local)
            return emptyList<NGSILDWarning>() to localLinkedEntities

        // the same filters are given twice to distributeQueryEntitiesOperation:
        // - in the query params, forwarded as is to the CSRs: object types are compacted with the request contexts,
        //   which are forwarded to the CSRs in the Link header
        // - in the EntitiesQuery, with expanded types, to select the matching CSRs
        // all the ids are queried with the union of all the object types (not each id with its own object types),
        // so a CSR may return a linked entity whose type is declared in the objectType of another relationship
        val ids = objectsWithTypes.keys
        val types = objectsWithTypes.values.flatten().toSet()
        val queryParams = LinkedMultiValueMap<String, String>().apply {
            add(QueryParameter.ID.key, ids.joinToString(","))
            add(QueryParameter.TYPE.key, types.toTypeSelection())
            // ponytail: one page sized to the ids, a CSR with a lower max limit answers with an error (warning)
            add(QueryParameter.LIMIT.key, ids.size.toString())
        }
        val linkedEntitiesQuery = EntitiesQueryFromGet(
            ids = ids,
            typeSelection = types.map { expandJsonLdTerm(it, entitiesQuery.contexts) }.toTypeSelection(),
            paginationQuery = PaginationQuery(0, ids.size),
            contexts = entitiesQuery.contexts
        )

        val (queryWarnings, remoteEntitiesWithCSR, _) =
            distributedEntityConsumptionService.distributeQueryEntitiesOperation(
                linkedEntitiesQuery,
                httpHeaders,
                queryParams
            )
        // a CSR could answer with entities that were not asked for
        val askedRemoteEntitiesWithCSR = remoteEntitiesWithCSR.map { (entities, csr) ->
            entities.filter { (it[NGSILD_ID_TERM] as? String)?.toUri() in ids } to csr
        }
        val (mergeWarnings, mergedEntities) =
            ContextSourceUtils.mergeEntitiesLists(localLinkedEntities, askedRemoteEntitiesWithCSR).toPair()

        return queryWarnings + mergeWarnings.orEmpty() to mergedEntities.orEmpty()
    }
}
