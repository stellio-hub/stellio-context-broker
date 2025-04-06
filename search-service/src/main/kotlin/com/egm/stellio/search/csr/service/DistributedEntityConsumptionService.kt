package com.egm.stellio.search.csr.service

import arrow.core.Either
import arrow.core.getOrNone
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import arrow.core.separateEither
import arrow.fx.coroutines.parMap
import com.egm.stellio.search.csr.model.CSRFilters
import com.egm.stellio.search.csr.model.ContextSourceRegistration.EntityInfo.Companion.addFilterForEntityInfo
import com.egm.stellio.search.csr.model.MiscellaneousPersistentWarning
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.csr.model.RevalidationFailedWarning
import com.egm.stellio.search.csr.model.SingleEntityInfoCSR
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.JsonUtils.deserializeAsList
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.util.CollectionUtils
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBodyOrNull
import org.springframework.web.reactive.function.client.awaitExchange
import java.net.URI

typealias QueryEntitiesResponse = Pair<List<CompactedEntity>, Int?>

@Service
class DistributedEntityConsumptionService(
    private val contextSourceRegistrationService: ContextSourceRegistrationService,
    private val applicationProperties: ApplicationProperties,
) {

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    suspend fun distributeRetrieveEntityOperation(
        id: URI,
        entitiesQuery: EntitiesQueryFromGet,
        httpHeaders: HttpHeaders,
        queryParams: MultiValueMap<String, String>
    ): Pair<List<NGSILDWarning>, List<CompactedEntityWithCSR>> {
        val csrFilters =
            CSRFilters(
                ids = setOf(id),
                operations = Operation.RETRIEVE_ENTITY.getMatchingOperations().toList(),
                attrs = entitiesQuery.attrs
            )

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(csrFilters)
            .flatMap { it.toSingleEntityInfoCSRList(csrFilters) }

        // we can add parMap(concurrency = X) if this trigger too much http connexion at the same time
        return matchingCSR.parMap { csr ->
            val response = retrieveEntityFromContextSource(
                httpHeaders,
                csr,
                csrFilters,
                id,
                queryParams
            )
            contextSourceRegistrationService.updateContextSourceStatus(csr, response.isRight())
            response.map { it?.let { it to csr } }
        }.separateEither()
            .let { (warnings, maybeResponses) ->
                warnings.toMutableList() to maybeResponses.filterNotNull()
            }
    }

    suspend fun retrieveEntityFromContextSource(
        httpHeaders: HttpHeaders,
        csr: SingleEntityInfoCSR,
        csrFilters: CSRFilters,
        id: URI,
        params: MultiValueMap<String, String>
    ): Either<NGSILDWarning, CompactedEntity?> = either {
        val path = "/ngsi-ld/v1/entities/$id"

        return kotlin.runCatching {
            getDistributedInformation(httpHeaders, csr, csrFilters, path, params).bind()
                .first?.deserializeAsMap().right()
        }.fold(
            onSuccess = { it },
            onFailure = { e ->
                logger.warn("Badly formed data received from CSR ${csr.id} at $path: ${e.message}")
                RevalidationFailedWarning(
                    "${csr.id} at $path returned badly formed data message: \"${e.cause}:${e.message}\"",
                    csr
                ).left()
            }
        )
    }

    suspend fun distributeQueryEntitiesOperation(
        entitiesQuery: EntitiesQueryFromGet,
        httpHeaders: HttpHeaders,
        queryParams: MultiValueMap<String, String>
    ): Triple<List<NGSILDWarning>, List<CompactedEntitiesWithCSR>, List<Int?>> {
        val csrFilters =
            CSRFilters(
                ids = entitiesQuery.ids,
                idPattern = entitiesQuery.idPattern,
                typeSelection = entitiesQuery.typeSelection,
                operations = Operation.QUERY_ENTITY.getMatchingOperations().toList(),
                attrs = entitiesQuery.attrs
            )

        val matchingCSR = contextSourceRegistrationService.getContextSourceRegistrations(csrFilters)
            .flatMap { it.toSingleEntityInfoCSRList(csrFilters) }

        return matchingCSR.parMap { csr ->
            val newParams = csr.information.first().entities?.first()?.let { entityInfo ->
                queryParams.addFilterForEntityInfo(entityInfo)
            } ?: queryParams
            val response = queryEntitiesFromContextSource(
                httpHeaders,
                csr,
                csrFilters,
                newParams
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
    }

    suspend fun queryEntitiesFromContextSource(
        httpHeaders: HttpHeaders,
        csr: SingleEntityInfoCSR,
        csrFilters: CSRFilters,
        params: MultiValueMap<String, String>
    ): Either<NGSILDWarning, QueryEntitiesResponse> = either {
        val path = "/ngsi-ld/v1/entities"

        return kotlin.runCatching {
            getDistributedInformation(httpHeaders, csr, csrFilters, path, params).bind().let { (response, headers) ->
                (response?.deserializeAsList() ?: emptyList()) to
                    // if count was not asked this will be null
                    headers.header(RESULTS_COUNT_HEADER).firstOrNull()?.toInt()
            }
                .right()
        }.fold(
            onSuccess = { it },
            onFailure = { e ->
                logger.warn("Badly formed data received from CSR ${csr.id} at $path: ${e.message}")
                RevalidationFailedWarning(
                    "${csr.id} at $path returned badly formed data message: \"${e.cause}:${e.message}\"",
                    csr
                ).left()
            }
        )
    }

    suspend fun getDistributedInformation(
        httpHeaders: HttpHeaders,
        csr: SingleEntityInfoCSR,
        csrFilters: CSRFilters,
        path: String,
        params: MultiValueMap<String, String>
    ): Either<NGSILDWarning, Pair<String?, ClientResponse.Headers>> = either {
        val contexts = getContextFromLinkHeaderOrDefault(httpHeaders, applicationProperties.contexts.core).getOrNull()!!
        val uri = URI("${csr.endpoint}$path")

        val queryParams = CollectionUtils.toMultiValueMap(params.toMutableMap())
        queryParams.remove(QueryParameter.GEOMETRY_PROPERTY.key)
        queryParams.remove(QueryParameter.OPTIONS.key) // only normalized request
        queryParams.remove(QueryParameter.LANG.key)
        csr.information.first().computeAttrsQueryParam(csrFilters, contexts)?.let {
            queryParams[QueryParameter.ATTRS.key] = it
        }
        val request = WebClient.create()
            .method(HttpMethod.GET)
            .uri { uriBuilder ->
                uriBuilder.scheme(uri.scheme)
                    .host(uri.host)
                    .port(uri.port)
                    .path(uri.path)
                    .queryParams(queryParams)
                    .build()
            }.headers { newHeaders ->
                httpHeaders.getOrNone(HttpHeaders.LINK).onSome { link -> newHeaders[HttpHeaders.LINK] = link }
            }

        return runCatching {
            val (statusCode, response, headers) = request.awaitExchange { response ->
                Triple(response.statusCode(), response.awaitBodyOrNull<String>(), response.headers())
            }
            when {
                statusCode.is2xxSuccessful -> {
                    logger.info("Successfully received data from CSR ${csr.id} at $uri")
                    (response to headers).right()
                }

                statusCode.isSameCodeAs(HttpStatus.NOT_FOUND) -> {
                    logger.info("CSR returned 404 at $uri: $response")
                    (null to headers).right()
                }

                else -> {
                    logger.warn("Error contacting CSR at $uri: $response")
                    MiscellaneousPersistentWarning(
                        "$uri returned an error $statusCode with response: $response",
                        csr
                    ).left()
                }
            }
        }.fold(
            onSuccess = { it },
            onFailure = { e ->
                logger.warn("Error contacting CSR at $uri: ${e.message}")
                logger.warn(e.stackTraceToString())
                MiscellaneousWarning(
                    "Error connecting to CSR at $uri: \"${e.cause}:${e.message}\"",
                    csr
                ).left()
            }
        )
    }
}
