package com.egm.stellio.shared.web

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.fasterxml.jackson.core.JsonParseException
import com.github.jsonldjava.core.JsonLdError
import org.springframework.core.codec.CodecException
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class ExceptionHandler {

    @ExceptionHandler
    fun transformErrorResponse(throwable: Throwable): ResponseEntity<String> =
        when (val cause = throwable.cause ?: throwable) {
            is AlreadyExistsException -> generateErrorResponse(
                HttpStatus.CONFLICT,
                AlreadyExistsResponse(cause.message)
            )
            is ResourceNotFoundException -> generateErrorResponse(
                HttpStatus.NOT_FOUND,
                ResourceNotFoundResponse(cause.message)
            )
            is InvalidRequestException -> generateErrorResponse(
                HttpStatus.BAD_REQUEST,
                InvalidRequestResponse(cause.message)
            )
            is CodecException -> generateErrorResponse(
                HttpStatus.BAD_REQUEST,
                JsonParseErrorResponse(cause.message ?: "There has been a problem during JSON parsing")
            )
            is BadRequestDataException -> generateErrorResponse(
                HttpStatus.BAD_REQUEST,
                BadRequestDataResponse(cause.message)
            )
            is JsonLdError -> generateErrorResponse(
                HttpStatus.BAD_REQUEST,
                JsonLdErrorResponse(cause.type.toString(), cause.message.orEmpty())
            )
            is JsonParseException -> generateErrorResponse(
                HttpStatus.BAD_REQUEST,
                JsonParseErrorResponse(cause.message ?: "There has been a problem during JSON parsing")
            )
            is AccessDeniedException -> generateErrorResponse(
                HttpStatus.FORBIDDEN,
                AccessDeniedResponse(cause.message)
            )
            is NotImplementedException -> generateErrorResponse(
                HttpStatus.NOT_IMPLEMENTED,
                NotImplementedResponse(cause.message)
            )
            is LdContextNotAvailableException -> generateErrorResponse(
                HttpStatus.SERVICE_UNAVAILABLE,
                LdContextNotAvailableResponse(cause.message)
            )
            else -> generateErrorResponse(
                HttpStatus.INTERNAL_SERVER_ERROR,
                InternalErrorResponse("$cause")
            )
        }

    private fun generateErrorResponse(status: HttpStatus, exception: Any) =
        ResponseEntity.status(status)
            .contentType(MediaType.APPLICATION_JSON)
            .body(serializeObject(exception))
}
