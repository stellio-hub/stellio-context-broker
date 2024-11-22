package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.right
import arrow.fx.coroutines.parMap
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AttributeRepresentation
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.EntityRepresentation
import com.egm.stellio.shared.model.EntityTypeSelection
import com.egm.stellio.shared.model.NgsiLdDataRepresentation
import com.egm.stellio.shared.model.NotAcceptableException
import com.egm.stellio.shared.model.PaginationQuery
import com.egm.stellio.shared.model.TooManyResultsException
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_TERM
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.QueryParam.Query.typeSelectionRegex
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.util.MimeTypeUtils
import org.springframework.util.MultiValueMap
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.time.format.DateTimeParseException
import java.util.Optional
import java.util.regex.Pattern

const val RESULTS_COUNT_HEADER = "NGSILD-Results-Count"
const val JSON_LD_CONTENT_TYPE = "application/ld+json"
const val GEO_JSON_CONTENT_TYPE = "application/geo+json"
const val JSON_MERGE_PATCH_CONTENT_TYPE = "application/merge-patch+json"


const val QUERY_PARAM_CONTAINED_BY: String = "containedBy"
const val QUERY_PARAM_JOIN: String = "join"
const val QUERY_PARAM_JOIN_LEVEL: String = "joinLevel"

val JSON_LD_MEDIA_TYPE = MediaType.valueOf(JSON_LD_CONTENT_TYPE)
val GEO_JSON_MEDIA_TYPE = MediaType.valueOf(GEO_JSON_CONTENT_TYPE)

val linkHeaderRegex: Regex =
    """<(.*)>;rel="http://www.w3.org/ns/json-ld#context";type="application/ld\+json"""".toRegex()

/**
 * As per 6.3.5, If the request verb is GET or DELETE, then the associated JSON-LD "@context" shall be obtained from a
 * Link header as mandated by JSON-LD, section 6.2.extract @context from Link header.
 * In the absence of such Link header, it returns the default JSON-LD @context.
 */
fun getContextFromLinkHeaderOrDefault(
    httpHeaders: HttpHeaders,
    coreContext: String
): Either<APIException, List<String>> =
    getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK))
        .map {
            if (it != null) addCoreContextIfMissing(listOf(it), coreContext)
            else listOf(coreContext)
        }

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
    """<$contextLink>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json""""

fun checkAndGetContext(
    httpHeaders: HttpHeaders,
    body: Map<String, Any>,
    coreContext: String
): Either<APIException, List<String>> = either {
    checkContentType(httpHeaders, body).bind()
    if (httpHeaders.contentType == MediaType.APPLICATION_JSON ||
        httpHeaders.contentType == MediaType.valueOf(JSON_MERGE_PATCH_CONTENT_TYPE)
    ) {
        getContextFromLinkHeaderOrDefault(httpHeaders, coreContext).bind()
    } else {
        val contexts = body.extractContexts()
        // kind of duplicate what is checked in checkContext but a bit more sure
        ensure(contexts.isNotEmpty()) {
            BadRequestDataException(
                "Request payload must contain @context term for a request having an application/ld+json content type"
            )
        }
        addCoreContextIfMissing(contexts, coreContext)
    }
}

suspend fun checkContentType(httpHeaders: HttpHeaders, body: List<Map<String, Any>>): Either<APIException, Unit> =
    either {
        body.parMap {
            checkContentType(httpHeaders, it).bind()
        }
    }.map { Unit.right() }

fun checkContentType(httpHeaders: HttpHeaders, body: Map<String, Any>): Either<APIException, Unit> {
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

/**
 * Returns a pair comprised of the compacted input (without a @context entry) and the applicable JSON-LD contexts
 * (with core @context added if it was missing)
 */
suspend fun extractPayloadAndContexts(
    requestBody: Mono<String>,
    httpHeaders: HttpHeaders,
    coreContext: String
): Either<APIException, Pair<CompactedEntity, List<String>>> = either {
    val body = requestBody.awaitFirst().deserializeAsMap()
        .checkNamesAreNgsiLdSupported().bind()
        .checkContentIsNgsiLdSupported().bind()
    val contexts = checkAndGetContext(httpHeaders, body, coreContext).bind()

    Pair(body.minus(JSONLD_CONTEXT), contexts)
}

fun Map<String, Any>.extractContexts(): List<String> =
    if (!this.containsKey(JSONLD_CONTEXT))
        emptyList()
    else if (this[JSONLD_CONTEXT] is List<*>)
        this[JSONLD_CONTEXT] as List<String>
    else if (this[JSONLD_CONTEXT] is String)
        listOf(this[JSONLD_CONTEXT] as String)
    else
        emptyList()

private val coreContextRegex = ".*ngsi-ld-core-context-v\\d+.\\d+.jsonld$".toRegex()

fun addCoreContextIfMissing(contexts: List<String>, coreContext: String): List<String> =
    when {
        contexts.isEmpty() -> listOf(coreContext)
        // to ensure that core @context, if present, comes last
        contexts.any { it.matches(coreContextRegex) } -> {
            val inlineCoreContext = contexts.find { it.matches(coreContextRegex) }!!
            contexts.filter { !it.matches(coreContextRegex) }.plus(inlineCoreContext)
        }
        // to check if the core @context is included / embedded in one of the link
        canExpandJsonLdKeyFromCore(contexts) -> contexts
        else -> contexts.plus(coreContext)
    }

/**
 * Utility but basic method to find if given contexts can resolve a known term from the core context.
 */
internal fun canExpandJsonLdKeyFromCore(contexts: List<String>): Boolean {
    val expandedType = JsonLdUtils.expandJsonLdTerm("datasetId", contexts)
    return expandedType == NGSILD_DATASET_ID_PROPERTY
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

fun expandTypeSelection(entityTypeSelection: EntityTypeSelection?, contexts: List<String>): EntityTypeSelection? =
    entityTypeSelection?.replace(typeSelectionRegex) {
        JsonLdUtils.expandJsonLdTerm(it.value.trim(), contexts)
    }

fun compactTypeSelection(entityTypeSelection: EntityTypeSelection, contexts: List<String>): EntityTypeSelection =
    entityTypeSelection.replace(typeSelectionRegex) {
        JsonLdUtils.compactTerm(it.value.trim(), contexts)
    }

fun parseAndExpandRequestParameter(requestParam: String?, contexts: List<String>): Set<String> =
    parseRequestParameter(requestParam)
        .map {
            JsonLdUtils.expandJsonLdTerm(it.trim(), contexts)
        }.toSet()

fun parseRepresentations(
    requestParams: MultiValueMap<String, String>,
    acceptMediaType: MediaType
): NgsiLdDataRepresentation {
    val optionsParam = requestParams.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
    val includeSysAttrs = optionsParam.contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
    val attributeRepresentation = optionsParam.contains(QUERY_PARAM_OPTIONS_KEYVALUES_VALUE)
        .let { if (it) AttributeRepresentation.SIMPLIFIED else AttributeRepresentation.NORMALIZED }
    val languageFilter = requestParams.getFirst(QUERY_PARAM_LANG)
    val entityRepresentation = EntityRepresentation.forMediaType(acceptMediaType)
    val geometryProperty =
        if (entityRepresentation == EntityRepresentation.GEO_JSON)
            requestParams.getFirst(QUERY_PARAM_GEOMETRY_PROPERTY) ?: NGSILD_LOCATION_TERM
        else null
    val timeproperty = requestParams.getFirst("timeproperty")

    return NgsiLdDataRepresentation(
        entityRepresentation,
        attributeRepresentation,
        includeSysAttrs,
        languageFilter,
        geometryProperty,
        timeproperty
    )
}

fun validateIdPattern(idPattern: String?): Either<APIException, String?> =
    idPattern?.let {
        runCatching {
            Pattern.compile(it)
        }.fold(
            { idPattern.right() },
            { BadRequestDataException("Invalid value for idPattern: $idPattern ($it)").left() }
        )
    } ?: Either.Right(null)

fun parsePaginationParameters(
    queryParams: MultiValueMap<String, String>,
    limitDefault: Int,
    limitMax: Int
): Either<APIException, PaginationQuery> {
    val count = queryParams.getFirst(QUERY_PARAM_COUNT)?.toBoolean() ?: false
    val offset = queryParams.getFirst(QUERY_PARAM_OFFSET)?.toIntOrNull() ?: 0
    val limit = queryParams.getFirst(QUERY_PARAM_LIMIT)?.toIntOrNull() ?: limitDefault
    if (!count && (limit <= 0 || offset < 0))
        return BadRequestDataException(
            "Offset must be greater than zero and limit must be strictly greater than zero"
        ).left()
    if (count && (limit < 0 || offset < 0))
        return BadRequestDataException("Offset and limit must be greater than zero").left()
    if (limit > limitMax)
        return TooManyResultsException(
            "You asked for $limit results, but the supported maximum limit is $limitMax"
        ).left()
    return PaginationQuery(offset, limit, count).right()
}

fun getApplicableMediaType(httpHeaders: HttpHeaders): Either<APIException, MediaType> =
    httpHeaders.accept.getApplicable()

/**
 * Return the applicable media type among JSON_LD_MEDIA_TYPE, GEO_JSON_MEDIA_TYPE and MediaType.APPLICATION_JSON.
 */
fun List<MediaType>.getApplicable(): Either<APIException, MediaType> {
    if (this.isEmpty())
        return MediaType.APPLICATION_JSON.right()
    MimeTypeUtils.sortBySpecificity(this)
    val mediaType = this.find {
        it.includes(MediaType.APPLICATION_JSON) || it.includes(JSON_LD_MEDIA_TYPE) || it.includes(GEO_JSON_MEDIA_TYPE)
    } ?: return NotAcceptableException("Unsupported Accept header value: ${this.joinToString(",")}").left()
    // as per 6.3.4, application/json has a higher precedence than application/ld+json
    return if (mediaType.includes(MediaType.APPLICATION_JSON))
        MediaType.APPLICATION_JSON.right()
    else
        mediaType.right()
}

fun acceptToMediaType(accept: String): MediaType =
    when (accept) {
        JSON_LD_CONTENT_TYPE -> JSON_LD_MEDIA_TYPE
        GEO_JSON_CONTENT_TYPE -> GEO_JSON_MEDIA_TYPE
        else -> MediaType.APPLICATION_JSON
    }

fun String.parseTimeParameter(errorMsg: String): Either<String, ZonedDateTime> =
    try {
        ZonedDateTime.parse(this).right()
    } catch (e: DateTimeParseException) {
        errorMsg.left()
    }
