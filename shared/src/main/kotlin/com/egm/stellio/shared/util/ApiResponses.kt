package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity

fun entityNotFoundMessage(entityId: String) = "Entity $entityId was not found"
fun entityOrAttrsNotFoundMessage(
    entityId: String,
    attrs: Set<String>
) = "Entity $entityId does not exist or it has none of the requested attributes : $attrs"

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

private fun generateErrorResponse(status: HttpStatus, exception: Any) =
    ResponseEntity.status(status)
        .contentType(MediaType.APPLICATION_JSON)
        .body(JsonUtils.serializeObject(exception))
