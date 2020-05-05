package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.core.JsonLdError
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.contentTypeOrNull
import reactor.core.publisher.Mono
import java.time.ZoneOffset
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
val JSON_LD_MEDIA_TYPE = MediaType.valueOf(JSON_LD_CONTENT_TYPE)

val ZONE_OFFSET = ZoneOffset.of("+02:00")

private fun isPostOrPatch(httpMethod: HttpMethod?): Boolean =
    listOf(HttpMethod.POST, HttpMethod.PATCH).contains(httpMethod)

/**
 * As per 6.3.5, extract @context from Link header. In the absence of such Link header, it returns the default
 * JSON-LD @context.
 */
fun extractContextFromLinkHeader(req: ServerRequest): String {
    return if (req.headers().header("Link").isNotEmpty() && req.headers().header("Link").get(0) != null)
        req.headers().header("Link")[0].split(";")[0].removePrefix("<").removeSuffix(">")
    else
        NgsiLdParsingUtils.NGSILD_CORE_CONTEXT
}

fun httpRequestPreconditions(request: ServerRequest, next: (ServerRequest) -> Mono<ServerResponse>): Mono<ServerResponse> {
    val contentType = request.headers().contentTypeOrNull()
    return if (isPostOrPatch(request.method()) && (contentType == null ||
                !listOf(MediaType.APPLICATION_JSON, JSON_LD_MEDIA_TYPE).contains(MediaType(contentType.type, contentType.subtype))))
        status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
            .bodyValue("Content-Type header must be one of 'application/json' or 'application/ld+json' (was $contentType)")
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

fun transformErrorResponse(throwable: Throwable, request: ServerRequest): Mono<ServerResponse> =
    when (throwable) {
        is AlreadyExistsException ->
            status(HttpStatus.CONFLICT)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(AlreadyExistsResponse(throwable.message))
        is ResourceNotFoundException ->
            status(HttpStatus.NOT_FOUND)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(ResourceNotFoundResponse(throwable.message))
        is BadRequestDataException ->
            badRequest()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(BadRequestDataResponse(throwable.message))
        is JsonLdError ->
            badRequest()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(JsonLdErrorResponse(throwable.type.toString(), throwable.message.orEmpty()))
        is JsonParseException ->
            badRequest()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(JsonParseErrorResponse(throwable.message ?: "There has been a problem during JSON parsing"))
        else ->
            status(HttpStatus.INTERNAL_SERVER_ERROR)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(InternalErrorResponse(throwable.message ?: "There has been an error during the operation execution"))
    }
