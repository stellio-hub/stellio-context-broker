package com.egm.stellio.shared.web

import com.apicatalog.jsonld.JsonLdError
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
import com.egm.stellio.shared.model.JsonLdErrorResponse
import com.egm.stellio.shared.model.JsonParseErrorResponse
import com.egm.stellio.shared.model.LdContextNotAvailableException
import com.egm.stellio.shared.model.LdContextNotAvailableResponse
import com.egm.stellio.shared.model.NonexistentTenantException
import com.egm.stellio.shared.model.NonexistentTenantResponse
import com.egm.stellio.shared.model.NotAcceptableResponse
import com.egm.stellio.shared.model.NotImplementedException
import com.egm.stellio.shared.model.NotImplementedResponse
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.ResourceNotFoundResponse
import com.egm.stellio.shared.model.UnsupportedMediaTypeResponse
import com.fasterxml.jackson.core.JsonParseException
import org.slf4j.LoggerFactory
import org.springframework.core.codec.CodecException
import org.springframework.http.HttpStatus
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.server.MethodNotAllowedException
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
                JsonLdErrorResponse(cause.code.toString(), cause.message.orEmpty())
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
            is MethodNotAllowedException ->
                ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body(cause.body)
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
