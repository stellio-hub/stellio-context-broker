package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import java.net.URI

fun invalidUriMessage(identifier: String) =
    "The supplied identifier was expected to be an URI but it is not: $identifier"

fun entityNotFoundMessage(entityId: String) = "Entity $entityId was not found"
fun entityAlreadyExistsMessage(entityId: String) = "Entity $entityId already exists"

fun typeNotFoundMessage(type: String) = "Type $type was not found"

fun entityOrAttrsNotFoundMessage(
    entityId: String,
    attrs: Set<String>
) = "Entity $entityId does not exist or it has none of the requested attributes : $attrs"

fun attributeNotFoundMessage(attributeName: String, datasetId: URI? = null) =
    if (datasetId == null)
        "Attribute $attributeName (default datasetId) was not found"
    else
        "Attribute $attributeName (datasetId: $datasetId) was not found"

fun attributeOrInstanceNotFoundMessage(
    attributeName: String,
    instanceId: String
) = "Instance $instanceId does not exist or attribute $attributeName was not found"

fun invalidCharacterInName(name: Any?) =
    "The JSON-LD object contains a member with invalid characters (4.6.2): $name"

fun invalidCharacterInContent(content: Any?) =
    "The JSON-LD object contains a member with invalid characters in value (4.6.4): $content"

fun invalidCharacterInScope(name: Any?) =
    "The JSON-LD object contains a scope with invalid characters (4.18): $name"

const val NULL_VALUE_IN_CONTENT = "The JSON-LD object contains a member with a null value (5.5.4)"

fun invalidTemporalInstanceMessage() =
    "One attribute instance is missing the required $NGSILD_OBSERVED_AT_PROPERTY temporal property"

const val INCONSISTENT_VALUES_IN_AGGREGATION_MESSAGE =
    "One of the aggregation methods tried to aggregate inconsistent types of values"

const val ENTITIY_CREATION_FORBIDDEN_MESSAGE = "User forbidden to create entity"
const val ENTITIY_READ_FORBIDDEN_MESSAGE = "User forbidden to read entity"
const val ENTITY_UPDATE_FORBIDDEN_MESSAGE = "User forbidden to modify entity"
const val ENTITY_DELETE_FORBIDDEN_MESSAGE = "User forbidden to delete entity"
const val ENTITY_ADMIN_FORBIDDEN_MESSAGE = "User forbidden to admin entity"
const val ENTITY_ALREADY_EXISTS_MESSAGE = "Entity already exists"
const val ENTITY_DOES_NOT_EXIST_MESSAGE = "Entity does not exist"

private val logger = LoggerFactory.getLogger("com.egm.stellio.shared.util.ApiResponses")

/**
 * this is globally duplicating what is in ExceptionHandler#transformErrorResponse()
 * but main code there should move here when we no longer raise business exceptions
 */
fun APIException.toErrorResponse(): ResponseEntity<*> =
    when (this) {
        is AlreadyExistsException ->
            generateErrorResponse(HttpStatus.CONFLICT, AlreadyExistsResponse(this.message))
        is ResourceNotFoundException ->
            generateErrorResponse(HttpStatus.NOT_FOUND, ResourceNotFoundResponse(this.message))
        is InvalidRequestException ->
            generateErrorResponse(HttpStatus.BAD_REQUEST, InvalidRequestResponse(this.message))
        is BadRequestDataException ->
            generateErrorResponse(HttpStatus.BAD_REQUEST, BadRequestDataResponse(this.message))
        is AccessDeniedException ->
            generateErrorResponse(HttpStatus.FORBIDDEN, AccessDeniedResponse(this.message))
        is NotImplementedException ->
            generateErrorResponse(HttpStatus.NOT_IMPLEMENTED, NotImplementedResponse(this.message))
        is LdContextNotAvailableException ->
            generateErrorResponse(HttpStatus.SERVICE_UNAVAILABLE, LdContextNotAvailableResponse(this.message))
        else -> generateErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, InternalErrorResponse("$cause"))
    }

private fun generateErrorResponse(status: HttpStatus, exception: ErrorResponse): ResponseEntity<*> {
    logger.info("Returning error ${exception.type} (${exception.detail})")
    return ResponseEntity.status(status)
        .contentType(MediaType.APPLICATION_JSON)
        .body(serializeObject(exception))
}

fun missingPathErrorResponse(errorMessage: String): ResponseEntity<*> {
    logger.info("Bad Request: $errorMessage")
    return BadRequestDataException(errorMessage).toErrorResponse()
}

fun buildQueryResponse(
    entities: List<CompactedJsonLdEntity>,
    count: Int,
    resourceUrl: String,
    queryParams: QueryParams,
    requestParams: MultiValueMap<String, String>,
    mediaType: MediaType,
    contextLink: String
): ResponseEntity<String> =
    buildQueryResponse(
        serializeObject(entities),
        count,
        resourceUrl,
        queryParams,
        requestParams,
        mediaType,
        contextLink
    )

fun buildQueryResponse(
    body: String,
    count: Int,
    resourceUrl: String,
    queryParams: QueryParams,
    requestParams: MultiValueMap<String, String>,
    mediaType: MediaType,
    contextLink: String
): ResponseEntity<String> {
    val prevAndNextLinks = PagingUtils.getPagingLinks(
        resourceUrl,
        requestParams,
        count,
        queryParams.offset,
        queryParams.limit
    )

    val responseHeaders = if (prevAndNextLinks.first != null && prevAndNextLinks.second != null)
        prepareGetSuccessResponse(mediaType, contextLink)
            .header(HttpHeaders.LINK, prevAndNextLinks.first)
            .header(HttpHeaders.LINK, prevAndNextLinks.second)

    else if (prevAndNextLinks.first != null)
        prepareGetSuccessResponse(mediaType, contextLink)
            .header(HttpHeaders.LINK, prevAndNextLinks.first)
    else if (prevAndNextLinks.second != null)
        prepareGetSuccessResponse(mediaType, contextLink)
            .header(HttpHeaders.LINK, prevAndNextLinks.second)
    else
        prepareGetSuccessResponse(mediaType, contextLink)

    return if (queryParams.count) responseHeaders.header(RESULTS_COUNT_HEADER, count.toString()).body(body)
    else responseHeaders.body(body)
}

fun prepareGetSuccessResponse(mediaType: MediaType, contextLink: String): ResponseEntity.BodyBuilder {
    return ResponseEntity.status(HttpStatus.OK)
        .apply {
            if (mediaType == JSON_LD_MEDIA_TYPE) {
                this.header(HttpHeaders.CONTENT_TYPE, JSON_LD_CONTENT_TYPE)
            } else {
                this.header(HttpHeaders.LINK, buildContextLinkHeader(contextLink))
                this.header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            }
        }
}
