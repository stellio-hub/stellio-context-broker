package com.egm.stellio.search.csr.service

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.util.*
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
                    JsonLdUtils.logger.info("Successfully received response from CSR at $uri")
                    response.right()
                }

                statusCode.isSameCodeAs(HttpStatus.NOT_FOUND) -> {
                    JsonLdUtils.logger.info("CSR returned 404 at $uri: $response")
                    null.right()
                }

                else -> {
                    JsonLdUtils.logger.warn("Error contacting CSR at $uri: $response")

                    MiscellaneousPersistentWarning(
                        "the CSR ${csr.id} returned an error $statusCode at : $uri response: \"$response\""
                    ).left()
                }
            }
        } catch (e: Exception) {
            when (e) {
                is DecodingException -> RevalidationFailedWarning(
                    "the CSR ${csr.id} as : $uri returned badly formed data message: \"${e.cause}:${e.message}\""
                )

                else -> MiscellaneousWarning(
                    "Error connecting to csr ${csr.id} at : $uri message : \"${e.cause}:${e.message}\""
                )
            }.left()
        }
    }
}
