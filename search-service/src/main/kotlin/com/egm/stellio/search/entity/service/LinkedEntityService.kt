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
import com.egm.stellio.shared.model.AttributeProjection
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.filterPickAndOmit
import com.egm.stellio.shared.model.getNestedProjectionsFor
import com.egm.stellio.shared.model.getRelationshipsNamesWithObjects
import com.egm.stellio.shared.model.getRelationshipsObjectsWithObjectTypes
import com.egm.stellio.shared.model.getRootAttributesToOmit
import com.egm.stellio.shared.model.getRootAttributesToPick
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

        val roots = compactedEntities.map {
            LinkedEntityNode(it[NGSILD_ID_TERM] as? String, it, entitiesQuery.pick, entitiesQuery.omit)
        }
        val (nodes, warnings) = enrichWithLinkedEntities(roots, entitiesQuery, httpHeaders, 1.toUInt()).bind()

        // in the flat representation, a Linked Entity reached through different paths is returned once,
        // with the union of the Attributes projected along each path
        val entities = if (linkedEntityQuery.join == JoinType.FLAT)
            nodes.take(roots.size).map { it.entity } +
                nodes.drop(roots.size).groupBy { it.id }.values.map { sameEntityNodes ->
                    sameEntityNodes.map { it.entity }.reduce { acc, entity -> acc + entity }
                }
        else nodes.map { it.entity }
        entities to warnings
    }

    /**
     * An entity projected along the path of Relationships leading to it, [pick] and [omit] being the projections
     * still to apply to its own Linked Entities.
     *
     * [id] is kept apart from [entity] since it may not have been picked.
     */
    private data class LinkedEntityNode(
        val id: String?,
        val entity: CompactedEntity,
        val pick: List<AttributeProjection>,
        val omit: List<AttributeProjection>
    ) {
        fun linkedEntityNodeKey(relationshipName: String, objectId: String): LinkedEntityNodeKey =
            LinkedEntityNodeKey(
                objectId,
                pick.getNestedProjectionsFor(relationshipName),
                omit.getNestedProjectionsFor(relationshipName)
            )
    }

    private data class LinkedEntityNodeKey(
        val id: String,
        val pick: List<AttributeProjection>,
        val omit: List<AttributeProjection>
    )

    /**
     * Returns the given nodes, with their Linked Entities inlined (inline join) or followed by their Linked Entities
     * (flat join).
     *
     * Linked Entities of a level are retrieved at once, then projected for each path reaching them.
     */
    private suspend fun enrichWithLinkedEntities(
        nodes: List<LinkedEntityNode>,
        entitiesQuery: EntitiesQuery,
        httpHeaders: HttpHeaders,
        currentLevel: UInt
    ): Either<APIException, Pair<List<LinkedEntityNode>, List<NGSILDWarning>>> = either {
        if (currentLevel > entitiesQuery.linkedEntityQuery!!.joinLevel)
            return (nodes to emptyList<NGSILDWarning>()).right()

        val childrenKeys = nodes.flatMap { node ->
            node.entity.getRelationshipsNamesWithObjects().flatMap { (relationshipName, objectIds) ->
                objectIds.map { node.linkedEntityNodeKey(relationshipName, it.toString()) }
            }
        }.distinct()
        if (childrenKeys.isEmpty())
            return (nodes to emptyList<NGSILDWarning>()).right()

        val localLinkedEntities =
            queryLocalLinkedEntities(childrenKeys.map { it.id.toUri() }.toSet(), entitiesQuery).bind()
        val (warnings, mergedLinkedEntities) = mergeWithRemoteLinkedEntities(
            localLinkedEntities,
            nodes.map { it.entity }.getRelationshipsObjectsWithObjectTypes(),
            entitiesQuery,
            httpHeaders
        )
        val linkedEntitiesById = mergedLinkedEntities.associateBy { it[NGSILD_ID_TERM] as String }

        // a Linked Entity with no member left after projection is not returned
        val children = childrenKeys.mapNotNull { key ->
            linkedEntitiesById[key.id]
                ?.filterPickAndOmit(key.pick.getRootAttributesToPick(), key.omit.getRootAttributesToOmit())
                ?.getOrNull()
                ?.let { key to LinkedEntityNode(key.id, it, key.pick, key.omit) }
        }

        val (enrichedChildren, nextLevelsWarnings) =
            enrichWithLinkedEntities(children.map { it.second }, entitiesQuery, httpHeaders, currentLevel.inc())
                .bind()

        val enrichedNodes = when (entitiesQuery.linkedEntityQuery!!.join) {
            JoinType.FLAT -> nodes + enrichedChildren
            JoinType.INLINE -> inlineLinkedEntities(nodes, children.map { it.first }.zip(enrichedChildren).toMap())
            // not possible, but it needs to be handled anyway
            else -> nodes
        }
        enrichedNodes to warnings + nextLevelsWarnings
    }

    private fun inlineLinkedEntities(
        nodes: List<LinkedEntityNode>,
        linkedEntitiesNodes: Map<LinkedEntityNodeKey, LinkedEntityNode>
    ): List<LinkedEntityNode> {
        // remove the inner contexts when inline mode, the context is the one from the linking entity
        val linkedEntities = linkedEntitiesNodes.mapValues { it.value.entity.minus(JSONLD_CONTEXT_KW) }
        return nodes.map { node ->
            node.copy(
                entity = node.entity.inlineLinkedEntities { relationshipName, objectId ->
                    linkedEntities[node.linkedEntityNodeKey(relationshipName, objectId)]
                }
            )
        }
    }

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
