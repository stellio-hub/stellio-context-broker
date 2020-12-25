package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils.extractContextFromInput
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.server.NotAcceptableStatusException
import java.time.ZonedDateTime
import java.time.format.DateTimeParseException
import java.util.*

fun String.parseTimeParameter(errorMsg: String): Either<String, ZonedDateTime> =
    try {
        ZonedDateTime.parse(this).right()
    } catch (e: DateTimeParseException) {
        errorMsg.left()
    }

const val JSON_LD_CONTENT_TYPE = "application/ld+json"
const val JSON_MERGE_PATCH_CONTENT_TYPE = "application/merge-patch+json"
const val QUERY_PARAM_ID: String = "id"
const val QUERY_PARAM_TYPE: String = "type"
const val QUERY_PARAM_FILTER: String = "q"
const val QUERY_PARAM_OPTIONS: String = "options"
const val QUERY_PARAM_OPTIONS_SYSATTRS_VALUE: String = "sysAttrs"
const val QUERY_PARAM_OPTIONS_KEYVALUES_VALUE: String = "keyValues"
val JSON_LD_MEDIA_TYPE = MediaType.valueOf(JSON_LD_CONTENT_TYPE)

/**
 * As per 6.3.5, extract @context from Link header. In the absence of such Link header, it returns the default
 * JSON-LD @context.
 */
fun getContextFromLinkHeaderOrDefault(httpHeaders: HttpHeaders): String {
    return getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)) ?: JsonLdUtils.NGSILD_CORE_CONTEXT
}

fun getContextFromLinkHeader(linkHeader: List<String>): String? {
    return if (linkHeader.isNotEmpty())
        linkHeader[0].split(";")[0].removePrefix("<").removeSuffix(">")
    else
        null
}

fun buildContextLinkHeader(contextLink: String): String =
    "<$contextLink>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\""

fun checkAndGetContext(httpHeaders: HttpHeaders, body: String): List<String> {
    return if (httpHeaders.contentType == MediaType.APPLICATION_JSON) {
        if (body.contains("@context"))
            throw BadRequestDataException(
                "Request payload must not contain @context term for a request having an application/json content type"
            )
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        listOf(contextLink)
    } else {
        if (getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)) != null)
            throw BadRequestDataException(
                "JSON-LD Link header must not be provided for a request having an application/ld+json content type"
            )
        val contexts = extractContextFromInput(body)
        if (contexts.isEmpty())
            throw BadRequestDataException(
                "Request payload must contain @context term for a request having an application/ld+json content type"
            )
        contexts
    }
}

enum class OptionsParamValue(val value: String) {
    TEMPORAL_VALUES("temporalValues")
}

fun hasValueInOptionsParam(options: Optional<String>, optionValue: OptionsParamValue): Boolean =
    options
        .map { it.split(",") }
        .filter { it.any { option -> option == optionValue.value } }
        .isPresent

fun parseAndExpandAttrsParameter(attrsParam: String?, contextLink: String): Set<String> =
    attrsParam
        ?.split(",")
        .orEmpty()
        .map {
            JsonLdUtils.expandJsonLdKey(it.trim(), contextLink)!!
        }
        .toSet()

fun getApplicableMediaType(httpHeaders: HttpHeaders): MediaType =
    httpHeaders.accept.getApplicable()

/**
 * Return the applicable media type among JSON_LD_MEDIA_TYPE and MediaType.APPLICATION_JSON.
 *
 * @throws NotAcceptableStatusException if none of the above media types are applicable
 */
fun List<MediaType>.getApplicable(): MediaType {
    if (this.isEmpty())
        return MediaType.APPLICATION_JSON
    MediaType.sortByQualityValue(this)
    val mediaType = this.find {
        it.includes(MediaType.APPLICATION_JSON) || it.includes(JSON_LD_MEDIA_TYPE)
    } ?: throw NotAcceptableStatusException(listOf(MediaType.APPLICATION_JSON, JSON_LD_MEDIA_TYPE))
    return if (mediaType.includes(JSON_LD_MEDIA_TYPE))
        JSON_LD_MEDIA_TYPE
    else
        MediaType.APPLICATION_JSON
}

fun buildGetSuccessResponse(mediaType: MediaType, contextLink: String): ResponseEntity.BodyBuilder {
    return ResponseEntity
        .status(HttpStatus.OK)
        .apply {
            if (mediaType == JSON_LD_MEDIA_TYPE) {
                this.header(HttpHeaders.CONTENT_TYPE, JSON_LD_CONTENT_TYPE)
            } else {
                this.header(HttpHeaders.LINK, buildContextLinkHeader(contextLink))
                this.header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            }
        }
}
