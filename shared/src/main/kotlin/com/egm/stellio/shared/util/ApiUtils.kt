package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.EntityTypeSelection
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_IRI
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.NGSILD_SCOPE_IRI
import com.egm.stellio.shared.model.NGSILD_SCOPE_TERM
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.model.NotAcceptableException
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.queryparameter.OptionsValue
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
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

val JSON_LD_MEDIA_TYPE = MediaType.valueOf(JSON_LD_CONTENT_TYPE)
val GEO_JSON_MEDIA_TYPE = MediaType.valueOf(GEO_JSON_CONTENT_TYPE)

val qPattern: Pattern = Pattern.compile("([^();|]+)")
val typeSelectionRegex: Regex = """([^(),;|]+)""".toRegex()
val scopeSelectionRegex: Regex = """([^(),;|]+)""".toRegex()
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

fun CompactedEntity.checkContentType(httpHeaders: HttpHeaders): Either<APIException, Map<String, Any>> =
    checkContentType(httpHeaders, this)
        .map { this }

fun checkContentType(httpHeaders: HttpHeaders, body: Map<String, Any>): Either<APIException, Unit> {
    if (httpHeaders.contentType == MediaType.APPLICATION_JSON ||
        httpHeaders.contentType == MediaType.valueOf(JSON_MERGE_PATCH_CONTENT_TYPE)
    ) {
        if (body.contains(JSONLD_CONTEXT_KW))
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
        if (!body.contains(JSONLD_CONTEXT_KW))
            return BadRequestDataException(
                "Request payload must contain @context term for a request having an application/ld+json content type"
            ).left()
    }
    return Unit.right()
}

/**
 * Checks the Link header with respect to Content-Type and rules defined 6.3.5
 */
fun checkLinkHeader(httpHeaders: HttpHeaders): Either<APIException, String?> = either {
    val linkHeader = getContextFromLinkHeader(httpHeaders.getOrEmpty(HttpHeaders.LINK)).bind()
    if (httpHeaders.contentType == JSON_LD_MEDIA_TYPE && linkHeader != null)
        BadRequestDataException(
            "JSON-LD Link header must not be provided for a request having an application/ld+json content type"
        ).left().bind()
    else
        linkHeader
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

    Pair(body.minus(JSONLD_CONTEXT_KW), contexts)
}

fun Map<String, Any>.extractContexts(): List<String> =
    if (!this.containsKey(JSONLD_CONTEXT_KW))
        emptyList()
    else if (this[JSONLD_CONTEXT_KW] is List<*>)
        this[JSONLD_CONTEXT_KW] as List<String>
    else if (this[JSONLD_CONTEXT_KW] is String)
        listOf(this[JSONLD_CONTEXT_KW] as String)
    else
        emptyList()

private val coreContextRegex = ".*ngsi-ld-core-context-v\\d+.\\d+.jsonld$".toRegex()

/**
 * Functional wrapper around #addCoreContextIfMissing, to return an Either and not throw an exception.
 */
fun addCoreContextIfMissingSafe(contexts: List<String>, coreContext: String): Either<APIException, List<String>> =
    runCatching {
        addCoreContextIfMissing(contexts, coreContext)
    }.fold(
        { it.right() },
        { it.toAPIException().left() }
    )

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
    val expandedType = expandJsonLdTerm("datasetId", contexts)
    return expandedType == NGSILD_DATASET_ID_IRI
}

fun hasValueInOptionsParam(
    optionsParam: Optional<String>,
    optionsValue: OptionsValue
): Either<APIException, Boolean> = either {
    optionsParam
        .map { it.split(",") }
        .orElse(emptyList())
        .map { OptionsValue.fromString(it).bind() }
        .any { it == optionsValue }
}

fun parseQueryParameter(queryParam: String?): Set<String> =
    queryParam
        ?.split(",")
        .orEmpty()
        .toSet()

fun expandTypeSelection(entityTypeSelection: EntityTypeSelection?, contexts: List<String>): EntityTypeSelection? =
    entityTypeSelection?.replace(typeSelectionRegex) {
        expandJsonLdTerm(it.value.trim(), contexts)
    }

fun compactTypeSelection(entityTypeSelection: EntityTypeSelection, contexts: List<String>): EntityTypeSelection =
    entityTypeSelection.replace(typeSelectionRegex) {
        JsonLdUtils.compactTerm(it.value.trim(), contexts)
    }

fun parseAndExpandQueryParameter(queryParam: String?, contexts: List<String>): Set<String> =
    parseQueryParameter(queryParam)
        .map {
            expandJsonLdTerm(it.trim(), contexts)
        }.toSet()

fun validateIdPattern(idPattern: String?): Either<APIException, String?> =
    idPattern?.let {
        runCatching {
            Pattern.compile(it)
        }.fold(
            { idPattern.right() },
            { BadRequestDataException("Invalid value for idPattern: $idPattern ($it)").left() }
        )
    } ?: Either.Right(null)

fun getApplicableMediaType(httpHeaders: HttpHeaders): Either<APIException, MediaType> =
    httpHeaders.accept.getApplicable()

/**
 * Return the applicable media type among JSON_LD_MEDIA_TYPE, GEO_JSON_MEDIA_TYPE, and MediaType.APPLICATION_JSON.
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

/**
 * Parse and expand pick/omit parameters according to NGSI-LD Attribute Projection Language ABNF grammar
 * as defined in section 4.21 of the NGSI-LD specification.
 */
fun parseAndExpandPickOmitParameters(
    queryParams: MultiValueMap<String, String>,
    contexts: List<String>
): Either<APIException, Pair<Set<String>, Set<String>>> = either {
    val pickParam = queryParams.getFirst(QueryParameter.PICK.key)
    val omitParam = queryParams.getFirst(QueryParameter.OMIT.key)

    val pick = pickParam?.let { pickValue ->
        parseAttributeProjectionList(pickValue, "pick", contexts).bind()
    } ?: emptySet()

    val omit = omitParam?.let { omitValue ->
        parseAttributeProjectionList(omitValue, "omit", contexts).bind()
    } ?: emptySet()

    Pair(pick, omit)
}

private fun parseAttributeProjectionList(
    paramValue: String,
    paramName: String,
    contexts: List<String>
): Either<APIException, Set<String>> = either {
    if (paramValue.isBlank()) {
        BadRequestDataException("The '$paramName' parameter cannot be empty").left().bind<Set<String>>()
    }

    val entityMembers = paramValue.split(",", "|").map { it.trim() }

    entityMembers.forEach { attrName ->
        attrName.checkNameIsNgsiLdSupported().bind()
    }

    entityMembers.map { attrName ->
        when (attrName) {
            NGSILD_ID_TERM -> JSONLD_ID_KW
            NGSILD_TYPE_TERM -> JSONLD_TYPE_KW
            NGSILD_SCOPE_TERM -> NGSILD_SCOPE_IRI
            else -> expandJsonLdTerm(attrName, contexts)
        }
    }.toSet()
}
