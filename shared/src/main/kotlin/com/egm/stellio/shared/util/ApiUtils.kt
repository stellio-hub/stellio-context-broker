package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils.extractContextFromInput
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import java.time.ZonedDateTime
import java.time.format.DateTimeParseException
import java.util.*

object ApiUtils {

    fun addContextToParsedObject(parsedObject: Map<String, Any>, contexts: List<String>): Map<String, Any> {
        return parsedObject.plus(Pair("@context", contexts))
    }
}

fun String.parseTimeParameter(errorMsg: String): Either<String, ZonedDateTime> =
    try {
        ZonedDateTime.parse(this).right()
    } catch (e: DateTimeParseException) {
        errorMsg.left()
    }

const val JSON_LD_CONTENT_TYPE = "application/ld+json"
const val JSON_MERGE_PATCH_CONTENT_TYPE = "application/merge-patch+json"
const val QUERY_PARAM_TYPE: String = "type"
const val QUERY_PARAM_FILTER: String = "q"
const val QUERY_PARAM_OPTIONS: String = "options"
const val QUERY_PARAM_OPTIONS_SYSATTRS_VALUE: String = "sysAttrs"
val JSON_LD_MEDIA_TYPE = MediaType.valueOf(JSON_LD_CONTENT_TYPE)

/**
 * As per 6.3.5, extract @context from Link header. In the absence of such Link header, it returns the default
 * JSON-LD @context.
 */
fun getContextFromLinkHeaderOrDefault(linkHeader: List<String>): String {
    return getContextFromLinkHeader(linkHeader) ?: JsonLdUtils.NGSILD_CORE_CONTEXT
}

fun getContextFromLinkHeaderOrDefault(httpHeaders: HttpHeaders): String {
    return getContextFromLinkHeader(httpHeaders.getOrEmpty("Link")) ?: JsonLdUtils.NGSILD_CORE_CONTEXT
}

fun getContextFromLinkHeader(linkHeader: List<String>): String? {
    return if (linkHeader.isNotEmpty())
        linkHeader[0].split(";")[0].removePrefix("<").removeSuffix(">")
    else
        null
}

fun checkAndGetContext(httpHeaders: HttpHeaders, body: String): List<String> {
    return if (httpHeaders.contentType == MediaType.APPLICATION_JSON) {
        if (body.contains("@context"))
            throw BadRequestDataException(
                "Request payload must not contain @context term for a request having an application/json content type"
            )
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders.getOrEmpty("Link"))
        listOf(contextLink)
    } else {
        if (getContextFromLinkHeader(httpHeaders.getOrEmpty("Link")) != null)
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
