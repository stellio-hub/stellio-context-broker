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

fun constructResponse(
    countAndEntities: Pair<Int, List<CompactedJsonLdEntity>>,
    resourceUrl: String,
    queryParams: QueryParams,
    requestParams: MultiValueMap<String, String>,
    mediaType: MediaType,
    contextLink: String
): ResponseEntity<String> {
    if (countAndEntities.second.isEmpty())
        return buildResponse(
            JsonUtils.serializeObject(emptyList<CompactedJsonLdEntity>()),
            countAndEntities.first,
            queryParams.count,
            Pair(null, null),
            mediaType, contextLink
        )
    else {
        val prevAndNextLinks = PagingUtils.getPagingLinks(
            resourceUrl,
            requestParams,
            countAndEntities.first,
            queryParams.offset,
            queryParams.limit
        )

        return buildResponse(
            JsonUtils.serializeObject(countAndEntities.second),
            countAndEntities.first,
            queryParams.count,
            prevAndNextLinks,
            mediaType, contextLink
        )
    }
}

fun constructResponse(
    countAndBody: Pair<Int, String>,
    queryParams: QueryParams,
    requestParams: MultiValueMap<String, String>,
    resourceUrl: String,
    mediaType: MediaType,
    contextLink: String
): ResponseEntity<String> {
    val prevAndNextLinks = PagingUtils.getPagingLinks(
        resourceUrl,
        requestParams,
        countAndBody.first,
        queryParams.offset,
        queryParams.limit
    )

    return buildResponse(
        countAndBody.second,
        countAndBody.first,
        queryParams.count,
        prevAndNextLinks,
        mediaType,
        contextLink
    )
}

fun buildResponse(
    body: String,
    resourcesCount: Int,
    count: Boolean,
    prevAndNextLinks: Pair<String?, String?>,
    mediaType: MediaType,
    contextLink: String
): ResponseEntity<String> {
    val responseHeaders = if (prevAndNextLinks.first != null && prevAndNextLinks.second != null)
        buildGetSuccessResponse(mediaType, contextLink)
            .header(HttpHeaders.LINK, prevAndNextLinks.first)
            .header(HttpHeaders.LINK, prevAndNextLinks.second)

    else if (prevAndNextLinks.first != null)
        buildGetSuccessResponse(mediaType, contextLink)
            .header(HttpHeaders.LINK, prevAndNextLinks.first)
    else if (prevAndNextLinks.second != null)
        buildGetSuccessResponse(mediaType, contextLink)
            .header(HttpHeaders.LINK, prevAndNextLinks.second)
    else
        buildGetSuccessResponse(mediaType, contextLink)

    return if (count) responseHeaders.header(RESULTS_COUNT_HEADER, resourcesCount.toString()).body(body)
    else responseHeaders.body(body)
}
