package com.egm.stellio.search.csr.service

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.search.csr.model.*
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.core.codec.DecodingException
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
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

        val request = WebClient.create()
            .method(HttpMethod.GET)
            .uri { uriBuilder ->
                uriBuilder.scheme(uri.scheme)
                    .host(uri.host)
                    .port(uri.port)
                    .path(uri.path)
                    .queryParams(params)
                    .build()
            }
            .header(HttpHeaders.LINK, httpHeaders.getFirst(HttpHeaders.LINK))
        return try {
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
                        "$uri returned an error $statusCode with response: \"$response\"",
                        csr
                    ).left()
                }
            }
        } catch (e: Exception) {
            logger.warn("Error contacting CSR at $uri: ${e.message}")
            logger.warn(e.stackTraceToString())
            when (e) {
                is DecodingException -> RevalidationFailedWarning(
                    "$uri returned badly formed data message: \"${e.cause}:${e.message}\"",
                    csr
                )

                else -> MiscellaneousWarning(
                    "Error connecting to $uri message : \"${e.cause}:${e.message}\"",
                    csr
                )
            }.left()
        }
    }
}