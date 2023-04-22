package com.egm.stellio.shared.web

import com.egm.stellio.shared.model.*
import com.fasterxml.jackson.core.JsonParseException
import com.github.jsonldjava.core.JsonLdError
import org.slf4j.LoggerFactory
import org.springframework.core.codec.CodecException
import org.springframework.http.HttpStatus
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.server.NotAcceptableStatusException
import org.springframework.web.server.UnsupportedMediaTypeStatusException

@RestControllerAdvice
class ExceptionHandler {

    private val logger = LoggerFactory.getLogger(javaClass)

    @ExceptionHandler
    fun transformErrorResponse(throwable: Throwable): ResponseEntity<ProblemDetail> =
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
            is BadRequestDataException -> generateErrorResponse(
                HttpStatus.BAD_REQUEST,
                BadRequestDataResponse(cause.message)
            )
            is JsonLdError -> generateErrorResponse(
                HttpStatus.BAD_REQUEST,
                JsonLdErrorResponse(cause.type.toString(), cause.message.orEmpty())
            )
            is JsonParseException, is CodecException -> generateErrorResponse(
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
            is UnsupportedMediaTypeStatusException -> generateErrorResponse(
                HttpStatus.UNSUPPORTED_MEDIA_TYPE,
                UnsupportedMediaTypeResponse(cause.message)
            )
            is NotAcceptableStatusException -> generateErrorResponse(
                HttpStatus.NOT_ACCEPTABLE,
                NotAcceptableResponse(cause.message)
            )
            is NonexistentTenantException -> generateErrorResponse(
                HttpStatus.NOT_FOUND,
                NonexistentTenantResponse(cause.message)
            )
            else -> generateErrorResponse(
                HttpStatus.INTERNAL_SERVER_ERROR,
                InternalErrorResponse("$cause")
            )
        }

    private fun generateErrorResponse(status: HttpStatus, exception: ErrorResponse): ResponseEntity<ProblemDetail> {
        logger.info("Returning error ${exception.type} (${exception.detail})")
        return ResponseEntity.status(status)
            .body(
                ProblemDetail.forStatusAndDetail(status, exception.detail).also {
                    it.title = exception.title
                    it.type = exception.type
                }
            )
    }
}
