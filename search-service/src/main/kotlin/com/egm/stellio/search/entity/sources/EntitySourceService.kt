package com.egm.stellio.search.entity.sources

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.separateEither
import arrow.fx.coroutines.parMap
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.csr.service.ContextSourceCaller
import com.egm.stellio.search.csr.service.ContextSourceRegistrationService
import com.egm.stellio.search.csr.service.ContextSourceUtils
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.entity.service.LinkedEntityService
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.filterAttributes
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.web.BaseHandler
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Service
import org.springframework.util.MultiValueMap
import java.net.URI

// todo find better package name and file name
@Service
class EntitySourceService(
    private val entityQueryService: EntityQueryService,
    private val contextSourceRegistrationService: ContextSourceRegistrationService,
    private val linkedEntityService: LinkedEntityService
) : BaseHandler() {
    // todo could also return Pair<Either<APIException, EntitiesWithCount>, List<NGSILDWarning>>
    // + more consistent with getEntity and let combined ApiException with warnings (no case for now)
    // - can't use .bind() easilly
    suspend fun getEntitiesFromSources(
        sub: Sub?,
        contexts: List<String>,
        entitiesQuery: EntitiesQueryFromGet,
        httpHeaders: HttpHeaders,
        queryParams: MultiValueMap<String, String>,
    ): Either<APIException, Triple<List<CompactedEntity>, Int, List<NGSILDWarning>>> = either {
        val csrFilters =
            CSRFilters(
                ids = entitiesQuery.ids,
                idPattern = entitiesQuery.idPattern,
                typeSelection = entitiesQuery.typeSelection,
                operations = listOf(
                    Operation.QUERY_ENTITY,
                    Operation.FEDERATION_OPS,
                    Operation.RETRIEVE_OPS,
                    Operation.REDIRECTION_OPS
                )
            )

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(csrFilters)

        val (entities, localCount) = entityQueryService.queryEntities(entitiesQuery, sub).bind()

        val filteredEntities = entities.filterAttributes(entitiesQuery.attrs, entitiesQuery.datasetId)

        val localEntities = compactEntities(filteredEntities, contexts)

        val (warnings, remoteEntitiesWithCSR, remoteCounts) = matchingCSR.parMap { csr ->
            val response = ContextSourceCaller.queryContextSourceEntities(
                httpHeaders,
                csr,
                queryParams
            )
            contextSourceRegistrationService.updateContextSourceStatus(csr, response.isRight())
            response.map { (entities, count) -> Triple(entities, csr, count) }
        }.separateEither()
            .let { (warnings, response) ->
                Triple(
                    warnings.toMutableList(),
                    response.map { (entities, csr, _) -> entities to csr },
                    response.map { (_, _, counts) -> counts }
                )
            }

        val maxCount = (remoteCounts + localCount).maxBy { it ?: 0 } ?: 0

        val mergedEntities = ContextSourceUtils.mergeEntitiesLists(
            localEntities,
            remoteEntitiesWithCSR
        ).toPair().let { (mergeWarnings, mergedEntities) ->
            mergeWarnings?.let { warnings.addAll(it) }
            mergedEntities ?: emptyList()
        }
        val mergedEntitiesWithLinkedEntities = mergedEntities.let {
            linkedEntityService.processLinkedEntities(it, entitiesQuery, sub).bind()
        }
        Triple(mergedEntitiesWithLinkedEntities, maxCount, warnings)
    }

    suspend fun getEntityFromSources(
        sub: Sub?,
        contexts: List<String>,
        entitiesQuery: EntitiesQueryFromGet,
        entityId: URI,
        httpHeaders: HttpHeaders,
        queryParams: MultiValueMap<String, String>
    ): Pair<Either<APIException, List<CompactedEntity>>, List<NGSILDWarning>> {
        val csrFilters =
            CSRFilters(
                ids = setOf(entityId),
                operations = listOf(
                    Operation.RETRIEVE_ENTITY,
                    Operation.FEDERATION_OPS,
                    Operation.RETRIEVE_OPS,
                    Operation.REDIRECTION_OPS
                )
            )

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(csrFilters)

        val localEntity = either {
            val expandedEntity = entityQueryService.queryEntity(entityId, sub).bind()
            expandedEntity.checkContainsAnyOf(entitiesQuery.attrs).bind()

            val filteredExpandedEntity = ExpandedEntity(
                expandedEntity.filterAttributes(entitiesQuery.attrs, entitiesQuery.datasetId)
            )
            compactEntity(filteredExpandedEntity, contexts)
        }

        // we can add parMap(concurrency = X) if this trigger too much http connexion at the same time
        val (warnings, remoteEntitiesWithCSR) = matchingCSR.parMap { csr ->
            val response = ContextSourceCaller.retrieveContextSourceEntity(
                httpHeaders,
                csr,
                entityId,
                queryParams
            )
            contextSourceRegistrationService.updateContextSourceStatus(csr, response.isRight())
            response.map { it?.let { it to csr } }
        }.separateEither()
            .let { (warnings, maybeResponses) ->
                warnings.toMutableList() to maybeResponses.filterNotNull()
            }

        val (mergeWarnings, mergedEntity) = ContextSourceUtils.mergeEntities(
            localEntity.getOrNull(),
            remoteEntitiesWithCSR
        ).toPair()

        mergeWarnings?.let { warnings.addAll(it) }

        if (mergedEntity == null) {
            val localError = localEntity.leftOrNull()!!
            return localError.left() to warnings
        }

        val mergedEntityWithLinkedEntities =
            linkedEntityService.processLinkedEntities(mergedEntity, entitiesQuery, sub)

        return mergedEntityWithLinkedEntities to warnings
    }
}
