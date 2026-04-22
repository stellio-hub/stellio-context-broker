package com.egm.stellio.search.csr.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.catch
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.MiscellaneousPersistentWarning
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.NGSILDWarning
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBodyOrNull
import org.springframework.web.reactive.function.client.awaitExchange
import java.net.URI

object ContextSourceHttpUtils {

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    suspend fun callGetOnCsr(
        uri: URI,
        httpHeaders: HttpHeaders,
        queryParams: MultiValueMap<String, String>,
        csr: ContextSourceRegistration,
    ): Either<NGSILDWarning, Pair<String?, ClientResponse.Headers>> = either {
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
                httpHeaders.getFirst(HttpHeaders.LINK)?.let { link -> newHeaders[HttpHeaders.LINK] = link }
            }

        return catch(
            {
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
                        logger.warn("CSR returned an error at $uri: $response")
                        MiscellaneousPersistentWarning(
                            "$uri returned an error $statusCode with response: $response",
                            csr
                        ).left()
                    }
                }
            },
            { e ->
                logger.warn("Error connecting to CSR at $uri: ${e.message}")
                logger.warn(e.stackTraceToString())
                MiscellaneousWarning(
                    "Error connecting to CSR at $uri: \"${e.cause}:${e.message}\"",
                    csr
                ).left()
            }
        )
    }
}
