package com.egm.stellio.search.csr.service

import arrow.core.Either
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
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.core.codec.DecodingException
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.util.CollectionUtils
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBodyOrNull
import org.springframework.web.reactive.function.client.awaitExchange
import java.net.URI

object ContextSourceCaller {
    val logger: Logger = LoggerFactory.getLogger(javaClass)

    suspend fun getDistributedInformation(
        httpHeaders: HttpHeaders,
        csr: ContextSourceRegistration,
        path: String,
        params: MultiValueMap<String, String>
    ): Either<NGSILDWarning, CompactedEntity?> = either {
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
            }
            .header(HttpHeaders.LINK, httpHeaders.getFirst(HttpHeaders.LINK))
        return runCatching {
            val (statusCode, response) = request
                .awaitExchange { response ->
                    response.statusCode() to response.awaitBodyOrNull<CompactedEntity>()
                }
            when {
                statusCode.is2xxSuccessful -> {
                    logger.info("Successfully received data from CSR ${csr.id} at $uri")
                    response.right()
                }

                statusCode.isSameCodeAs(HttpStatus.NOT_FOUND) -> {
                    logger.info("CSR returned 404 at $uri: $response")
                    null.right()
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
                if (e is DecodingException) {
                    RevalidationFailedWarning(
                        "$uri returned badly formed data message: \"${e.cause}:${e.message}\"",
                        csr
                    )
                } else {
                    MiscellaneousWarning(
                        "Error connecting to $uri message : \"${e.cause}:${e.message}\"",
                        csr
                    )
                }.left()
            }
        )
    }
}
