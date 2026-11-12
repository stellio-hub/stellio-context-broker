package com.egm.stellio.search.csr.service

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.catch
import arrow.core.raise.either
import arrow.core.right
import arrow.core.separateEither
import arrow.fx.coroutines.parMap
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.DistributedTemporalQueryResult
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.csr.model.RevalidationFailedWarning
import com.egm.stellio.search.csr.model.SingleEntityInfoCSR
import com.egm.stellio.search.csr.util.CompactedEntitiesWithCSR
import com.egm.stellio.search.csr.util.CompactedEntityWithCSR
import com.egm.stellio.search.csr.util.ContextSourceHttpUtils
import com.egm.stellio.search.csr.util.getTemporalPagination
import com.egm.stellio.search.csr.util.mergeTemporalPagination
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.model.TemporalPagination
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.ErrorMessages.Csr.contextSourceBadlyFormedMessage
import com.egm.stellio.shared.util.JsonUtils.deserializeAsList
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Service
import org.springframework.util.CollectionUtils
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.client.ClientResponse
import java.net.URI

@Service
class DistributedTemporalEntityConsumptionService(
    private val contextSourceRegistrationService: ContextSourceRegistrationService,
    private val applicationProperties: ApplicationProperties,
) {

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    suspend fun distributeRetrieveTemporalEntityOperation(
        id: URI,
        temporalEntitiesQuery: TemporalEntitiesQuery,
        httpHeaders: HttpHeaders,
        queryParams: MultiValueMap<String, String>
    ): DistributedTemporalQueryResult<CompactedEntityWithCSR> {
        val csrFilters = CSRFilters(
            ids = setOf(id),
            operations = Operation.RETRIEVE_TEMPORAL.getMatchingOperations().toList(),
            attrs = temporalEntitiesQuery.entitiesQuery.attrs
        )

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(csrFilters)
            .flatMap { it.toSingleEntityInfoCSRList(csrFilters) }

        return matchingCSR.parMap { csr ->
            val response = retrieveTemporalEntityFromContextSource(
                httpHeaders,
                csr,
                csrFilters,
                id,
                queryParams
            )
            contextSourceRegistrationService.updateContextSourceStatus(csr, response.isRight())
            response.map { (entity, pagination) -> entity?.let { it to csr } to pagination }
        }.separateEither()
            .let { (warnings, maybeResponses) ->
                val entities = maybeResponses.mapNotNull { it.first }
                DistributedTemporalQueryResult(
                    warnings = warnings,
                    entities = entities,
                    count = if (entities.isEmpty()) 0 else 1,
                    pagination = maybeResponses.map { it.second }
                        .mergeTemporalPagination(temporalEntitiesQuery.temporalQuery)
                )
            }
    }

    suspend fun retrieveTemporalEntityFromContextSource(
        httpHeaders: HttpHeaders,
        csr: SingleEntityInfoCSR,
        csrFilters: CSRFilters,
        id: URI,
        params: MultiValueMap<String, String>
    ): Either<NGSILDWarning, Pair<CompactedEntity?, TemporalPagination>> = either {
        val path = "/ngsi-ld/v1/temporal/entities/$id"

        return catch(
            {
                getTemporalDistributedInformation(httpHeaders, csr, csrFilters, path, params).bind()
                    .let { (response, headers) ->
                        response?.deserializeAsMap() to if (response == null) TemporalPagination()
                        else getTemporalPagination(headers.header(HttpHeaders.LINK))
                    }.right()
            },
            { e ->
                logger.warn("Badly formed data received from CSR ${csr.id} at $path: ${e.message}")
                RevalidationFailedWarning(
                    contextSourceBadlyFormedMessage(csr.id, path, e.cause.toString(), e.message),
                    csr
                ).left()
            }
        )
    }

    suspend fun distributeQueryTemporalEntitiesOperation(
        temporalEntitiesQuery: TemporalEntitiesQueryFromGet,
        httpHeaders: HttpHeaders,
        queryParams: MultiValueMap<String, String>
    ): DistributedTemporalQueryResult<CompactedEntitiesWithCSR> {
        val entitiesQuery = temporalEntitiesQuery.entitiesQuery
        val csrFilters = CSRFilters(
            ids = entitiesQuery.ids,
            idPattern = entitiesQuery.idPattern,
            typeSelection = entitiesQuery.typeSelection,
            operations = Operation.QUERY_TEMPORAL.getMatchingOperations().toList(),
            attrs = entitiesQuery.attrs
        )

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(csrFilters)
            .flatMap { it.toSingleEntityInfoCSRList(csrFilters) }

        return matchingCSR.parMap { csr ->
            val response = queryTemporalEntitiesFromContextSource(
                httpHeaders,
                csr,
                csrFilters,
                queryParams
            )
            contextSourceRegistrationService.updateContextSourceStatus(csr, response.isRight())
            response.map { (entities, count, pagination) -> Triple(entities to csr, count, pagination) }
        }.separateEither()
            .let { (warnings, response) ->
                DistributedTemporalQueryResult(
                    warnings = warnings,
                    entities = response.map { it.first },
                    count = response.mapNotNull { it.second }.maxOrNull() ?: 0,
                    pagination = response.map { it.third }
                        .mergeTemporalPagination(temporalEntitiesQuery.temporalQuery)
                )
            }
    }

    suspend fun queryTemporalEntitiesFromContextSource(
        httpHeaders: HttpHeaders,
        csr: SingleEntityInfoCSR,
        csrFilters: CSRFilters,
        params: MultiValueMap<String, String>
    ): Either<NGSILDWarning, Triple<List<CompactedEntity>, Int?, TemporalPagination>> = either {
        val path = "/ngsi-ld/v1/temporal/entities"

        return catch(
            {
                getTemporalDistributedInformation(httpHeaders, csr, csrFilters, path, params).bind()
                    .let { (response, headers) ->
                        Triple(
                            response?.deserializeAsList() ?: emptyList(),
                            headers.header(RESULTS_COUNT_HEADER).firstOrNull()?.toInt(),
                            if (response == null) TemporalPagination()
                            else getTemporalPagination(headers.header(HttpHeaders.LINK))
                        )
                    }
                    .right()
            },
            { e ->
                logger.warn("Badly formed data received from CSR ${csr.id} at $path: ${e.message}")
                RevalidationFailedWarning(
                    contextSourceBadlyFormedMessage(csr.id, path, e.cause.toString(), e.message),
                    csr
                ).left()
            }
        )
    }

    suspend fun getTemporalDistributedInformation(
        httpHeaders: HttpHeaders,
        csr: SingleEntityInfoCSR,
        csrFilters: CSRFilters,
        path: String,
        params: MultiValueMap<String, String>
    ): Either<NGSILDWarning, Pair<String?, ClientResponse.Headers>> = either {
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).getOrNull()!!
        val uri = URI("${csr.endpoint}$path")

        val queryParams = CollectionUtils.toMultiValueMap(params.toMutableMap())
        queryParams.remove(QueryParameter.LANG.key)
        csr.information.first().computeAttrsQueryParam(csrFilters, contexts)?.let {
            queryParams[QueryParameter.ATTRS.key] = it
        }

        return ContextSourceHttpUtils.callGetOnCsr(uri, httpHeaders, queryParams, csr)
    }
}
