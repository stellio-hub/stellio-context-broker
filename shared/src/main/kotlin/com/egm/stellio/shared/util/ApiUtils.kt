package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.InvalidNgsiLdPayloadException
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.badRequest
import org.springframework.web.reactive.function.server.ServerResponse.status
import org.springframework.web.reactive.function.server.contentTypeOrNull
import reactor.core.publisher.Mono
import java.time.OffsetDateTime
import java.time.format.DateTimeParseException

fun String.parseTimeParameter(errorMsg: String) : OffsetDateTime =
    try {
        OffsetDateTime.parse(this)
    } catch (e: DateTimeParseException) {
        throw BadRequestDataException(errorMsg)
    }

const val JSON_LD_CONTENT_TYPE = "application/ld+json"
val JSON_LD_MEDIA_TYPE = MediaType.valueOf(JSON_LD_CONTENT_TYPE)

fun httpRequestPreconditions(request: ServerRequest, next: (ServerRequest) -> Mono<ServerResponse>) : Mono<ServerResponse> {
    val contentType = request.headers().contentTypeOrNull()
    return if (contentType == null ||
        !listOf(MediaType.APPLICATION_JSON, JSON_LD_MEDIA_TYPE).contains(contentType))
        status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
            .bodyValue("Content-Type header must be one of 'application/json' or 'application/ld+json' (was $contentType)")
    else
        next(request)
}

fun transformErrorResponse(throwable: Throwable, request: ServerRequest) : Mono<ServerResponse> =
    when (throwable) {
        is BadRequestDataException -> badRequest().contentType(MediaType.APPLICATION_JSON).bodyValue(throwable.message.orEmpty())
        is InvalidNgsiLdPayloadException -> badRequest().contentType(MediaType.APPLICATION_JSON).bodyValue(throwable.message.orEmpty())
        else -> status(HttpStatus.INTERNAL_SERVER_ERROR).contentType(MediaType.APPLICATION_JSON).bodyValue(throwable.message.orEmpty())
    }
