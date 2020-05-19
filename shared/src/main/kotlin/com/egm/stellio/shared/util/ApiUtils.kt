package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.*
import org.springframework.web.reactive.function.server.ServerResponse.*
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.time.format.DateTimeParseException
import java.util.*

object ApiUtils {

    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    fun serializeObject(input: Any): String {
        return mapper.writeValueAsString(input)
    }

    fun addContextToParsedObject(parsedObject: Map<String, Any>, contexts: List<String>): Map<String, Any> {
        return parsedObject.plus(Pair("@context", contexts))
    }

    fun parseSubscription(content: String): Subscription =
        mapper.readValue(content, Subscription::class.java)

    fun parseNotification(content: String): Notification =
        mapper.readValue(content, Notification::class.java)
}

fun String.parseTimeParameter(errorMsg: String): ZonedDateTime =
    try {
        ZonedDateTime.parse(this)
    } catch (e: DateTimeParseException) {
        throw BadRequestDataException(errorMsg)
    }

const val JSON_LD_CONTENT_TYPE = "application/ld+json"
const val JSON_MERGE_PATCH_CONTENT_TYPE = "application/merge-patch+json"
val JSON_LD_MEDIA_TYPE = MediaType.valueOf(JSON_LD_CONTENT_TYPE)
val JSON_MERGE_PATCH_MEDIA_TYPE = MediaType.valueOf(JSON_MERGE_PATCH_CONTENT_TYPE)

/**
 * As per 6.3.5, extract @context from Link header. In the absence of such Link header, it returns the default
 * JSON-LD @context.
 */
fun extractContextFromLinkHeader(linkHeader: List<String>): String {
    return if (linkHeader.isNotEmpty())
        linkHeader[0].split(";")[0].removePrefix("<").removeSuffix(">")
    else
        NgsiLdParsingUtils.NGSILD_CORE_CONTEXT
}

fun List<MediaType>.isAcceptable(): Boolean {
    return this.any {
        it == MediaType("*", "*") ||
        it == MediaType("application", "*") ||
        it == MediaType("application", "json") ||
        it == MediaType("application", "ld+json")
    }
}

// TODO: add check for allowed methods and return 405
fun httpRequestPreconditions(request: ServerRequest, next: (ServerRequest) -> Mono<ServerResponse>): Mono<ServerResponse> {
    return when (request.method()) {
        HttpMethod.GET -> httpGetRequestPreconditions(request, next)
        HttpMethod.POST -> httpPostRequestPreconditions(request, next)
        HttpMethod.PATCH -> httpPatchRequestPreconditions(request, next)
        else -> next(request)
    }
}

private fun httpGetRequestPreconditions(request: ServerRequest, next: (ServerRequest) -> Mono<ServerResponse>): Mono<ServerResponse> {
    val accept = request.headers().accept()

    return if (accept.isNotEmpty() && !accept.isAcceptable())
        status(HttpStatus.NOT_ACCEPTABLE).build()
    else
        next(request)
}

private fun httpPostRequestPreconditions(request: ServerRequest, next: (ServerRequest) -> Mono<ServerResponse>): Mono<ServerResponse> {
    val contentType = request.headers().contentTypeOrNull()
    val contentLength = request.headers().contentLengthOrNull()

    return if (contentLength == null)
            status(HttpStatus.LENGTH_REQUIRED).build()
        else if (contentType == null || !listOf(MediaType.APPLICATION_JSON, JSON_LD_MEDIA_TYPE).contains(MediaType(contentType.type, contentType.subtype)))
            status(HttpStatus.UNSUPPORTED_MEDIA_TYPE).build()
        else
            next(request)
}


private fun httpPatchRequestPreconditions(request: ServerRequest, next: (ServerRequest) -> Mono<ServerResponse>): Mono<ServerResponse> {
    val contentType = request.headers().contentTypeOrNull()
    val contentLength = request.headers().contentLengthOrNull()

    return if (contentLength == null)
            status(HttpStatus.LENGTH_REQUIRED).build()
        else if (contentType == null || !listOf(
                MediaType.APPLICATION_JSON,
                JSON_LD_MEDIA_TYPE,
                JSON_MERGE_PATCH_MEDIA_TYPE
            ).contains(MediaType(contentType.type, contentType.subtype))
        )
            status(HttpStatus.UNSUPPORTED_MEDIA_TYPE).build()
        else
            next(request)
}

enum class OptionsParamValue(val value: String) {
    TEMPORAL_VALUES("temporalValues")
}

fun hasValueInOptionsParam(options: Optional<String>, optionValue: OptionsParamValue): Boolean =
    options
        .map { it.split(",") }
        .filter { it.any { option -> option == optionValue.value } }
        .isPresent