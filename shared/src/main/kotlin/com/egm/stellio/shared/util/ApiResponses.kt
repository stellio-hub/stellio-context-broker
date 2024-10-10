package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.AccessDeniedResponse
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.AlreadyExistsResponse
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.BadRequestDataResponse
import com.egm.stellio.shared.model.ErrorResponse
import com.egm.stellio.shared.model.InternalErrorResponse
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.model.InvalidRequestResponse
import com.egm.stellio.shared.model.LdContextNotAvailableException
import com.egm.stellio.shared.model.LdContextNotAvailableResponse
import com.egm.stellio.shared.model.NotImplementedException
import com.egm.stellio.shared.model.NotImplementedResponse
import com.egm.stellio.shared.model.OperationNotSupportedException
import com.egm.stellio.shared.model.OperationNotSupportedResponse
import com.egm.stellio.shared.model.PaginationQuery
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.ResourceNotFoundResponse
import com.egm.stellio.shared.model.TooManyResultsException
import com.egm.stellio.shared.model.TooManyResultsResponse
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
const val ENTITY_REMOVE_OWNERSHIP_FORBIDDEN_MESSAGE = "User forbidden to remove ownership of entity"
const val ENTITY_ALREADY_EXISTS_MESSAGE = "Entity already exists"
const val ENTITY_DOES_NOT_EXIST_MESSAGE = "Entity does not exist"

private val logger = LoggerFactory.getLogger("com.egm.stellio.shared.util.ApiResponses")

/**
 * this is globally duplicating what is in ExceptionHandler#transformErrorResponse()
 * but main code there should move here when we no longer raise business exceptions
 */
// todo put in ApiException File?
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
        is OperationNotSupportedException ->
            generateErrorResponse(HttpStatus.BAD_REQUEST, OperationNotSupportedResponse(this.message))
        is AccessDeniedException ->
            generateErrorResponse(HttpStatus.FORBIDDEN, AccessDeniedResponse(this.message))
        is NotImplementedException ->
            generateErrorResponse(HttpStatus.NOT_IMPLEMENTED, NotImplementedResponse(this.message))
        is LdContextNotAvailableException ->
            generateErrorResponse(HttpStatus.SERVICE_UNAVAILABLE, LdContextNotAvailableResponse(this.message))
        is TooManyResultsException ->
            generateErrorResponse(HttpStatus.FORBIDDEN, TooManyResultsResponse(this.message))
        is ContextSourceRequestException ->
            generateErrorResponse(this.status, ContextSourceRequestResponse(this.message))
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
    entities: Any,
    count: Int,
    resourceUrl: String,
    paginationQuery: PaginationQuery,
    requestParams: MultiValueMap<String, String>,
    mediaType: MediaType,
    contexts: List<String>
): ResponseEntity<String> =
    buildQueryResponse(
        serializeObject(entities),
        count,
        resourceUrl,
        paginationQuery,
        requestParams,
        mediaType,
        contexts
    )

fun buildQueryResponse(
    body: String,
    count: Int,
    resourceUrl: String,
    paginationQuery: PaginationQuery,
    requestParams: MultiValueMap<String, String>,
    mediaType: MediaType,
    contexts: List<String>
): ResponseEntity<String> {
    val prevAndNextLinks = PagingUtils.getPagingLinks(
        resourceUrl,
        requestParams,
        count,
        paginationQuery.offset,
        paginationQuery.limit
    )

    val responseHeaders = if (prevAndNextLinks.first != null && prevAndNextLinks.second != null)
        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .header(HttpHeaders.LINK, prevAndNextLinks.first)
            .header(HttpHeaders.LINK, prevAndNextLinks.second)

    else if (prevAndNextLinks.first != null)
        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .header(HttpHeaders.LINK, prevAndNextLinks.first)
    else if (prevAndNextLinks.second != null)
        prepareGetSuccessResponseHeaders(mediaType, contexts)
            .header(HttpHeaders.LINK, prevAndNextLinks.second)
    else
        prepareGetSuccessResponseHeaders(mediaType, contexts)

    return if (paginationQuery.count) responseHeaders.header(RESULTS_COUNT_HEADER, count.toString()).body(body)
    else responseHeaders.body(body)
}

fun prepareGetSuccessResponseHeaders(mediaType: MediaType, contexts: List<String>): ResponseEntity.BodyBuilder =
    ResponseEntity.status(HttpStatus.OK)
        .apply {
            if (mediaType == JSON_LD_MEDIA_TYPE) {
                this.header(HttpHeaders.CONTENT_TYPE, JSON_LD_CONTENT_TYPE)
            } else {
                this.header(HttpHeaders.LINK, buildContextLinkHeader(contexts.first()))
                this.header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            }
        }
