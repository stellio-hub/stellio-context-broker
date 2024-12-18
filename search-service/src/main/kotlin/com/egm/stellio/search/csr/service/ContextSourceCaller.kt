package com.egm.stellio.search.csr.service

import arrow.core.Either
import arrow.core.getOrNone
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.MiscellaneousPersistentWarning
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.csr.model.RevalidationFailedWarning
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.JsonUtils.deserializeAsList
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.util.CollectionUtils
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBodyOrNull
import org.springframework.web.reactive.function.client.awaitExchange
import java.net.URI

typealias QueryEntitiesResponse = Pair<List<CompactedEntity>, Int?>

object ContextSourceCaller {
    val logger: Logger = LoggerFactory.getLogger(javaClass)

    suspend fun retrieveContextSourceEntity(
        httpHeaders: HttpHeaders,
        csr: ContextSourceRegistration,
        id: URI,
        params: MultiValueMap<String, String>
    ): Either<NGSILDWarning, CompactedEntity?> = either {
        val path = "/ngsi-ld/v1/entities/$id"

        return kotlin.runCatching {
            getDistributedInformation(httpHeaders, csr, path, params).bind().first?.deserializeAsMap().right()
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

    suspend fun queryContextSourceEntities(
        httpHeaders: HttpHeaders,
        csr: ContextSourceRegistration,
        params: MultiValueMap<String, String>
    ): Either<NGSILDWarning, QueryEntitiesResponse> = either {
        val path = "/ngsi-ld/v1/entities"

        return kotlin.runCatching {
            getDistributedInformation(httpHeaders, csr, path, params).bind().let { (response, headers) ->
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
        csr: ContextSourceRegistration,
        path: String,
        params: MultiValueMap<String, String>
    ): Either<NGSILDWarning, Pair<String?, ClientResponse.Headers>> = either {
        val uri = URI("${csr.endpoint}$path")

        val queryParams = CollectionUtils.toMultiValueMap(params.toMutableMap())
        queryParams.remove(QueryParameter.GEOMETRY_PROPERTY.key)
        queryParams.remove(QueryParameter.OPTIONS.key) // only normalized request
        queryParams.remove(QueryParameter.LANG.key)

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
