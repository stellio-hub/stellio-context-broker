package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap

fun entityNotFoundMessage(entityId: String) = "Entity $entityId was not found"

fun attributeNotFoundMessage(attributeName: String) = "Attribute $attributeName was not found"

fun instanceNotFoundMessage(instanceId: String) = "Instance $instanceId was not found"

fun entityOrAttrsNotFoundMessage(
    entityId: String,
    attrs: Set<String>
) = "Entity $entityId does not exist or it has none of the requested attributes : $attrs"

const val ENTITIY_CREATION_FORBIDDEN_MESSAGE = "User forbidden to create entity"
const val ENTITY_UPDATE_FORBIDDEN_MESSAGE = "User forbidden to modify entity"
const val ENTITY_DELETE_FORBIDDEN_MESSAGE = "User forbidden to delete entity"
const val ENTITY_ALREADY_EXISTS_MESSAGE = "Entity already exists"
const val ENTITY_DOES_NOT_EXIST_MESSAGE = "Entity does not exist"

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

private fun generateErrorResponse(status: HttpStatus, exception: Any): ResponseEntity<*> =
    ResponseEntity.status(status)
        .contentType(MediaType.APPLICATION_JSON)
        .body(JsonUtils.serializeObject(exception))

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
        JsonUtils.serializeObject(entities),
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
