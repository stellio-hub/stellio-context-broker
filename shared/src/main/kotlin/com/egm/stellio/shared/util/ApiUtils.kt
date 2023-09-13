package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import arrow.fx.coroutines.parMap
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.extractContextFromInput
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.util.MimeTypeUtils
import org.springframework.util.MultiValueMap
import org.springframework.web.server.NotAcceptableStatusException
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.time.format.DateTimeParseException
import java.util.Optional
import java.util.regex.Pattern

fun String.parseTimeParameter(errorMsg: String): Either<String, ZonedDateTime> =
    try {
        ZonedDateTime.parse(this).right()
    } catch (e: DateTimeParseException) {
        errorMsg.left()
    }

const val RESULTS_COUNT_HEADER = "NGSILD-Results-Count"
const val JSON_LD_CONTENT_TYPE = "application/ld+json"
const val JSON_MERGE_PATCH_CONTENT_TYPE = "application/merge-patch+json"
const val QUERY_PARAM_COUNT: String = "count"
const val QUERY_PARAM_OFFSET: String = "offset"
const val QUERY_PARAM_LIMIT: String = "limit"
const val QUERY_PARAM_ID: String = "id"
const val QUERY_PARAM_TYPE: String = "type"
const val QUERY_PARAM_ID_PATTERN: String = "idPattern"
const val QUERY_PARAM_ATTRS: String = "attrs"
const val QUERY_PARAM_Q: String = "q"
const val QUERY_PARAM_SCOPEQ: String = "scopeQ"
const val QUERY_PARAM_OPTIONS: String = "options"
const val QUERY_PARAM_OPTIONS_SYSATTRS_VALUE: String = "sysAttrs"
const val QUERY_PARAM_OPTIONS_KEYVALUES_VALUE: String = "keyValues"
const val QUERY_PARAM_OPTIONS_NOOVERWRITE_VALUE: String = "noOverwrite"
const val QUERY_PARAM_OPTIONS_OBSERVEDAT_VALUE: String = "observedAt"
val JSON_LD_MEDIA_TYPE = MediaType.valueOf(JSON_LD_CONTENT_TYPE)

val qPattern: Pattern = Pattern.compile("([^();|]+)")
val typeSelectionRegex: Regex = """([^(),;|]+)""".toRegex()
val scopeSelectionRegex: Regex = """([^(),;|]+)""".toRegex()
val linkHeaderRegex: Regex =
    """<(.*)>;rel="http://www.w3.org/ns/json-ld#context";type="application/ld\+json"""".toRegex()

/**
 * As per 6.3.5, extract @context from Link header. In the absence of such Link header, it returns the default
 * JSON-LD @context.
 */
fun getContextFromLinkHeaderOrDefault(httpHeaders: HttpHeaders): Either<APIException, String> =
    getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK))
        .map { it ?: JsonLdUtils.NGSILD_CORE_CONTEXT }

fun getContextFromLinkHeader(linkHeader: List<String>): Either<APIException, String?> =
    if (linkHeader.isNotEmpty())
        linkHeaderRegex.find(linkHeader[0].replace(" ", ""))?.groupValues?.let {
            if (it.isEmpty() || it[1].isEmpty())
                BadRequestDataException("Badly formed Link header: ${linkHeader[0]}").left()
            else it[1].right()
        } ?: BadRequestDataException("Badly formed Link header: ${linkHeader[0]}").left()
    else
        Either.Right(null)

fun buildContextLinkHeader(contextLink: String): String =
    "<$contextLink>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\""

fun checkAndGetContext(
    httpHeaders: HttpHeaders,
    body: Map<String, Any>
): Either<APIException, List<String>> = either {
    checkContext(httpHeaders, body).bind()
    if (httpHeaders.contentType == MediaType.APPLICATION_JSON ||
        httpHeaders.contentType == MediaType.valueOf(JSON_MERGE_PATCH_CONTENT_TYPE)
    ) {
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders).bind()
        listOf(contextLink)
    } else {
        val contexts = extractContextFromInput(body)
        // kind of duplicate what is checked in checkContext but a bit more sure
        if (contexts.isEmpty())
            BadRequestDataException(
                "Request payload must contain @context term for a request having an application/ld+json content type"
            ).left().bind()
        contexts
    }
}

suspend fun checkContext(httpHeaders: HttpHeaders, body: List<Map<String, Any>>): Either<APIException, Unit> =
    either {
        body.parMap {
            checkContext(httpHeaders, it).bind()
        }
    }.map { Unit.right() }

fun checkContext(httpHeaders: HttpHeaders, body: Map<String, Any>): Either<APIException, Unit> {
    if (httpHeaders.contentType == MediaType.APPLICATION_JSON ||
        httpHeaders.contentType == MediaType.valueOf(JSON_MERGE_PATCH_CONTENT_TYPE)
    ) {
        if (body.contains(JSONLD_CONTEXT))
            return BadRequestDataException(
                "Request payload must not contain @context term for a request having an application/json content type"
            ).left()
    } else {
        getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)).map {
            if (it != null)
                return BadRequestDataException(
                    "JSON-LD Link header must not be provided for a request having an application/ld+json content type"
                ).left()
        }
        if (!body.contains(JSONLD_CONTEXT))
            return BadRequestDataException(
                "Request payload must contain @context term for a request having an application/ld+json content type"
            ).left()
    }
    return Unit.right()
}

suspend fun extractPayloadAndContexts(
    requestBody: Mono<String>,
    httpHeaders: HttpHeaders
): Either<APIException, Pair<CompactedJsonLdEntity, List<String>>> = either {
    val body = requestBody.awaitFirst().deserializeAsMap()
        .checkNamesAreNgsiLdSupported().bind()
        .checkContentIsNgsiLdSupported().bind()
    val contexts = checkAndGetContext(httpHeaders, body).bind()

    Pair(body, contexts)
}

enum class OptionsParamValue(val value: String) {
    TEMPORAL_VALUES("temporalValues"),
    AUDIT("audit"),
    AGGREGATED_VALUES("aggregatedValues")
}

fun hasValueInOptionsParam(options: Optional<String>, optionValue: OptionsParamValue): Boolean =
    options
        .map { it.split(",") }
        .filter { it.any { option -> option == optionValue.value } }
        .isPresent

fun parseRequestParameter(requestParam: String?): Set<String> =
    requestParam
        ?.split(",")
        .orEmpty()
        .toSet()

fun parseAndExpandTypeSelection(type: String?, contextLink: String): String? =
    parseAndExpandTypeSelection(type, listOf(contextLink))

fun parseAndExpandTypeSelection(type: String?, contexts: List<String>): String? =
    type?.replace(typeSelectionRegex) {
        JsonLdUtils.expandJsonLdTerm(it.value.trim(), contexts)
    }

fun parseAndExpandRequestParameter(requestParam: String?, contextLink: String): Set<String> =
    parseRequestParameter(requestParam)
        .map {
            JsonLdUtils.expandJsonLdTerm(it.trim(), contextLink)
        }.toSet()

fun parsePaginationParameters(
    queryParams: MultiValueMap<String, String>,
    limitDefault: Int,
    limitMax: Int,
    isAskingForCount: Boolean = false
): Either<APIException, Pair<Int, Int>> {
    val offset = queryParams.getFirst(QUERY_PARAM_OFFSET)?.toIntOrNull() ?: 0
    val limit = queryParams.getFirst(QUERY_PARAM_LIMIT)?.toIntOrNull() ?: limitDefault
    if (!isAskingForCount && (limit <= 0 || offset < 0))
        return BadRequestDataException(
            "Offset must be greater than zero and limit must be strictly greater than zero"
        ).left()
    if (isAskingForCount && (limit < 0 || offset < 0))
        return BadRequestDataException("Offset and limit must be greater than zero").left()
    if (limit > limitMax)
        return BadRequestDataException(
            "You asked for $limit results, but the supported maximum limit is $limitMax"
        ).left()
    return Pair(offset, limit).right()
}

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
    MimeTypeUtils.sortBySpecificity(this)
    val mediaType = this.find {
        it.includes(MediaType.APPLICATION_JSON) || it.includes(JSON_LD_MEDIA_TYPE)
    } ?: throw NotAcceptableStatusException(listOf(MediaType.APPLICATION_JSON, JSON_LD_MEDIA_TYPE))
    // as per 6.3.4, application/json has a higher precedence than application/ld+json
    return if (mediaType.includes(MediaType.APPLICATION_JSON))
        MediaType.APPLICATION_JSON
    else
        JSON_LD_MEDIA_TYPE
}

suspend fun parseQueryParams(
    pagination: Pair<Int, Int>,
    requestParams: MultiValueMap<String, String>,
    contextLink: String
): Either<APIException, QueryParams> = either {
    val ids = requestParams.getFirst(QUERY_PARAM_ID)?.split(",").orEmpty().toListOfUri().toSet()
    val type = parseAndExpandTypeSelection(requestParams.getFirst(QUERY_PARAM_TYPE), contextLink)
    val idPattern = requestParams.getFirst(QUERY_PARAM_ID_PATTERN)?.also { idPattern ->
        runCatching {
            Pattern.compile(idPattern)
        }.onFailure {
            BadRequestDataException("Invalid value for idPattern: $idPattern ($it)").left().bind()
        }
    }

    /**
     * Decoding query parameters is not supported by default so a call to a decode function was added query
     * with the right parameters values
     */
    val q = requestParams.getFirst(QUERY_PARAM_Q)?.decode()
    val scopeQ = requestParams.getFirst(QUERY_PARAM_SCOPEQ)
    val count = requestParams.getFirst(QUERY_PARAM_COUNT)?.toBoolean() ?: false
    val attrs = parseAndExpandRequestParameter(requestParams.getFirst(QUERY_PARAM_ATTRS), contextLink)
    val includeSysAttrs = requestParams.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
        .contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
    val useSimplifiedRepresentation = requestParams.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
        .contains(QUERY_PARAM_OPTIONS_KEYVALUES_VALUE)
    val (offset, limit) = parsePaginationParameters(
        requestParams,
        pagination.first,
        pagination.second,
        count
    ).bind()

    val geoQuery = parseGeoQueryParameters(requestParams.toSingleValueMap(), contextLink).bind()

    QueryParams(
        ids = ids,
        type = type,
        idPattern = idPattern,
        q = q,
        scopeQ = scopeQ,
        limit = limit,
        offset = offset,
        count = count,
        attrs = attrs,
        includeSysAttrs = includeSysAttrs,
        useSimplifiedRepresentation = useSimplifiedRepresentation,
        geoQuery = geoQuery,
        context = contextLink
    )
}
